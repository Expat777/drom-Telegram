"""
DAG telegram_ingest — раз в 10 минут читает выбранные телеграм-каналы
и складывает сырые сообщения в telegram_raw.

Два режима сбора (выбирается автоматически):
  1. Telethon (user-API) — если заданы TELEGRAM_API_ID/HASH и есть .session.
     Достаёт полную историю, работает и с приватными каналами.
  2. Веб-превью t.me/s/<канал> — если API-доступов НЕТ. Без авторизации и
     без .session, но только публичные каналы и лишь последние посты со
     страницы превью. Достаточно для дашборда.

Про Telethon .session: первый вход интерактивный (ввод кода), в headless-
контейнере не сработает — сессию генерят локально (scripts/gen_telegram_session.py)
и кладут в ./telegram_session/.
"""
from __future__ import annotations

import asyncio
import json
import random
import time
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
from sqlalchemy import text

from airflow import DAG
from airflow.operators.python import PythonOperator

from common.config import telegram_channels, telegram_creds, telegram_proxy
from common.db import connection

# сколько последних сообщений тянуть с канала за один прогон (Telethon-режим)
LIMIT_PER_CHANNEL = 50

# веб-режим: заголовки и вежливые задержки/ретраи
_WEB_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept-Language": "ru,en;q=0.9",
}
_WEB_RETRIES = 3


# ----------------------------------------------------------------------
# Режим 1: Telethon (user-API)
# ----------------------------------------------------------------------
async def _collect_telethon(channels: list[str]) -> list[dict]:
    from telethon import TelegramClient

    api_id, api_hash, session = telegram_creds()
    rows: list[dict] = []
    async with TelegramClient(session, api_id, api_hash) as client:
        for ch in channels:
            try:
                async for msg in client.iter_messages(ch, limit=LIMIT_PER_CHANNEL):
                    if not msg.message:
                        continue
                    rows.append({
                        "channel": ch,
                        "message_id": msg.id,
                        "text": msg.message,
                        "url": f"https://t.me/{ch}/{msg.id}",
                        "raw": {"date": msg.date.isoformat() if msg.date else None},
                    })
            except Exception as e:  # noqa: BLE001
                print(f"[tg] ошибка по каналу {ch}: {e}")
    print(f"[tg] (telethon) собрано сообщений: {len(rows)}")
    return rows


# ----------------------------------------------------------------------
# Режим 2: публичное веб-превью t.me/s/<канал> (без авторизации)
# ----------------------------------------------------------------------
def _fetch(url: str) -> str | None:
    for attempt in range(1, _WEB_RETRIES + 1):
        try:
            resp = requests.get(url, timeout=25, headers=_WEB_HEADERS,
                                 proxies=telegram_proxy())
            if resp.status_code == 429 or resp.status_code >= 500:
                raise requests.HTTPError(f"status {resp.status_code}")
            resp.raise_for_status()
            return resp.text
        except Exception as e:  # noqa: BLE001
            wait = 2 ** attempt + random.uniform(0, 1)
            print(f"[tg] попытка {attempt}/{_WEB_RETRIES} для {url}: {e}; жду {wait:.1f}s")
            if attempt < _WEB_RETRIES:
                time.sleep(wait)
    print(f"[tg] не смог скачать {url}")
    return None


def _collect_web(channels: list[str]) -> list[dict]:
    rows: list[dict] = []
    for i, ch in enumerate(channels):
        if i:
            time.sleep(random.uniform(1.5, 3.5))   # вежливая задержка
        html = _fetch(f"https://t.me/s/{ch}")
        if html is None:
            continue
        soup = BeautifulSoup(html, "html.parser")
        msgs = soup.select(".tgme_widget_message")
        collected = 0
        for m in msgs:
            node = m.select_one(".tgme_widget_message_text")
            body = node.get_text(" ", strip=True) if node else ""
            if not body:
                continue
            # data-post = "<channel>/<message_id>"
            post = m.get("data-post") or ""
            mid = post.split("/")[-1]
            if not mid.isdigit():
                continue
            time_el = m.select_one(".tgme_widget_message_date time")
            date = time_el.get("datetime") if time_el else None
            rows.append({
                "channel": ch,
                "message_id": int(mid),
                "text": body,
                "url": f"https://t.me/{ch}/{mid}",
                "raw": {"date": date},
            })
            collected += 1
        print(f"[tg] (web) {ch}: сообщений с текстом {collected} из {len(msgs)}")
    print(f"[tg] (web) собрано сообщений: {len(rows)}")
    return rows


# ----------------------------------------------------------------------
def _collect() -> list[dict]:
    """Выбирает режим: Telethon при наличии API-доступов, иначе веб-превью."""
    api_id, api_hash, _ = telegram_creds()
    channels = telegram_channels()
    if not channels:
        print("[tg] TELEGRAM_CHANNELS пуст — пропуск")
        return []
    if api_id and api_hash:
        return asyncio.run(_collect_telethon(channels))
    print("[tg] API-доступов нет — собираю через публичное веб-превью t.me/s/")
    return _collect_web(channels)


def ingest(**_):
    rows = _collect()
    if not rows:
        return
    now = datetime.now(timezone.utc)
    with connection() as conn:
        for r in rows:
            # ON CONFLICT — не дублируем одно и то же сообщение
            conn.execute(
                text("""
                    INSERT INTO telegram_raw
                        (channel, message_id, text, url, raw, collected_at)
                    VALUES (:channel, :message_id, :text, :url, :raw, :ts)
                    ON CONFLICT (channel, message_id) DO NOTHING
                """),
                {"channel": r["channel"], "message_id": r["message_id"],
                 "text": r["text"], "url": r["url"],
                 "raw": json.dumps(r["raw"], ensure_ascii=False), "ts": now},
            )
    print(f"[tg] обработано строк: {len(rows)}")


with DAG(
    dag_id="telegram_ingest",
    start_date=datetime(2024, 1, 1),
    schedule="*/10 * * * *",
    catchup=False,
    tags=["ingest", "telegram"],
) as dag:
    PythonOperator(task_id="ingest_telegram", python_callable=ingest)
