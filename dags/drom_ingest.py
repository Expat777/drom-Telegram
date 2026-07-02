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
import random
import time
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
from sqlalchemy import text

from airflow import DAG
from airflow.operators.python import PythonOperator

from common.db import connection

# Охват: обходим несколько городов, а не одну общую ленту /all/.
# У drom пагинация PATH-ОВАЯ: /all/ -> page1, /all/page2/ -> page2 и т.д.
# (частая ошибка — ?page=N; он отдаёт ту же первую страницу, охвата не даёт).
DROM_CITIES = [
    "moscow", "spb", "novosibirsk", "ekaterinburg",
    "vladivostok", "krasnodar", "kazan", "rostov-na-donu",
]
PAGES = 3   # страниц пагинации на каждый город


def _page_url(city: str, page: int) -> str:
    base = f"https://auto.drom.ru/{city}/all/"
    return base if page == 1 else f"{base}page{page}/"

# Антибот drom: не долбим сервер в лоб.
#   - ротация User-Agent (пул реальных браузеров);
#   - случайная задержка между страницами;
#   - ретраи с экспоненциальным бэкоффом на сетевые ошибки и 429/5xx.
USER_AGENTS = [
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
]
MAX_RETRIES = 3
DELAY_RANGE = (2.0, 5.0)   # секунды между запросами страниц


def _headers() -> dict:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "ru,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }


def _fetch(url: str) -> str | None:
    """Скачивает страницу с ретраями/бэкоффом. Возвращает HTML или None."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, timeout=25, headers=_headers())
            if resp.status_code == 429 or resp.status_code >= 500:
                raise requests.HTTPError(f"status {resp.status_code}")
            resp.raise_for_status()
            resp.encoding = "windows-1251"   # ВАЖНО: drom в windows-1251
            return resp.text
        except Exception as e:  # noqa: BLE001
            wait = 2 ** attempt + random.uniform(0, 1)
            print(f"[drom] попытка {attempt}/{MAX_RETRIES} не удалась для {url}: "
                  f"{e}; жду {wait:.1f}s")
            if attempt < MAX_RETRIES:
                time.sleep(wait)
    print(f"[drom] не смог скачать {url} после {MAX_RETRIES} попыток")
    return None


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
    first_request = True
    for city in DROM_CITIES:
        for page in range(1, PAGES + 1):
            url = _page_url(city, page)
            # вежливая задержка перед каждым запросом, кроме самого первого
            if not first_request:
                time.sleep(random.uniform(*DELAY_RANGE))
            first_request = False

            html = _fetch(url)
            if html is None:
                continue
            soup = BeautifulSoup(html, "html.parser")
            cards = soup.select('[data-ftid="bulls-list_bull"]')
            for c in cards:
                parsed = _parse_card(c)
                if parsed:
                    items.append(parsed)
            print(f"[drom] {city} p{page}: карточек {len(cards)}")
    # дедуп по url в рамках одного прогона (одно объявление может попасть
    # в ленту нескольких городов/страниц)
    uniq = {i["url"]: i for i in items}
    print(f"[drom] собрано {len(items)}, уникальных: {len(uniq)}")
    return list(uniq.values())


def ingest(**_):
    rows = scrape_drom()
    if not rows:
        print("[drom] нечего вставлять")
        return
    now = datetime.now(timezone.utc)
    with connection() as conn:
        for r in rows:
            # ON CONFLICT (url) — не копим дубли при частых прогонах,
            # но обновляем снимок карточки (цена/пробег могли измениться).
            conn.execute(
                text("""
                    INSERT INTO drom_raw (url, raw, collected_at)
                    VALUES (:url, :raw, :ts)
                    ON CONFLICT (url) DO UPDATE SET
                        raw = EXCLUDED.raw,
                        collected_at = EXCLUDED.collected_at
                """),
                {"url": r["url"], "raw": json.dumps(r, ensure_ascii=False),
                 "ts": now},
            )
    print(f"[drom] обработано строк (insert/update): {len(rows)}")


with DAG(
    dag_id="drom_ingest",
    start_date=datetime(2024, 1, 1),
    schedule="*/10 * * * *",
    catchup=False,
    tags=["ingest", "drom"],
) as dag:
    PythonOperator(task_id="ingest_drom", python_callable=ingest)
