"""
DAG drom_ingest — раз в 10 минут тянет объявления с drom.ru
и кладёт их КАК ЕСТЬ (структурированные поля карточки) в таблицу drom_raw.

Особенности drom (проверено на живой странице):
  - страницы в кодировке windows-1251 (requests не угадывает -> задаём явно);
  - карточка: div[data-ftid="bulls-list_bull"], ссылка a[data-ftid="bull_title"];
  - URL содержит город/марку/модель: auto.drom.ru/<city>/<brand>/<model>/<id>.html;
  - цена: [data-ftid="bull_price"]; год: в заголовке; пробег: последний desc-item.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
from sqlalchemy import text

from airflow import DAG
from airflow.operators.python import PythonOperator

from common.db import connection

# базовые страницы выдачи; PAGES — сколько страниц пагинации обходить
DROM_LIST_URLS = ["https://auto.drom.ru/all/"]
PAGES = 3
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
}


def _parse_card(card) -> dict | None:
    a = card.select_one('a[data-ftid="bull_title"]') or card.find("a", href=True)
    if not a or not a.get("href"):
        return None
    price_el = card.select_one('[data-ftid="bull_price"]')
    descs = [d.get_text(" ", strip=True)
             for d in card.select('[data-ftid="bull_description-item"]')]
    return {
        "url": a["href"],
        "title": a.get_text(" ", strip=True),   # "Лада Веста, 2019"
        "price_text": price_el.get_text(" ", strip=True) if price_el else None,
        "desc_items": descs,                    # включает пробег "45 000 км"
    }


def scrape_drom() -> list[dict]:
    items: list[dict] = []
    for base in DROM_LIST_URLS:
        for page in range(1, PAGES + 1):
            url = base if page == 1 else f"{base}?page={page}"
            try:
                resp = requests.get(url, timeout=25, headers=HEADERS)
                resp.raise_for_status()
                resp.encoding = "windows-1251"   # ВАЖНО
            except Exception as e:  # noqa: BLE001
                print(f"[drom] не смог скачать {url}: {e}")
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            cards = soup.select('[data-ftid="bulls-list_bull"]')
            for c in cards:
                parsed = _parse_card(c)
                if parsed:
                    items.append(parsed)
            print(f"[drom] {url}: карточек {len(cards)}")
    # дедуп по url в рамках одного прогона
    uniq = {i["url"]: i for i in items}
    print(f"[drom] всего уникальных: {len(uniq)}")
    return list(uniq.values())


def ingest(**_):
    rows = scrape_drom()
    if not rows:
        print("[drom] нечего вставлять")
        return
    now = datetime.now(timezone.utc)
    with connection() as conn:
        for r in rows:
            conn.execute(
                text("INSERT INTO drom_raw (url, raw, collected_at) "
                     "VALUES (:url, :raw, :ts)"),
                {"url": r["url"], "raw": json.dumps(r, ensure_ascii=False),
                 "ts": now},
            )
    print(f"[drom] вставлено строк: {len(rows)}")


with DAG(
    dag_id="drom_ingest",
    start_date=datetime(2024, 1, 1),
    schedule="*/10 * * * *",
    catchup=False,
    tags=["ingest", "drom"],
) as dag:
    PythonOperator(task_id="ingest_drom", python_callable=ingest)
