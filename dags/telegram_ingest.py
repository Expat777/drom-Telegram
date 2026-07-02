"""
DAG telegram_ingest — раз в 10 минут читает выбранные телеграм-каналы
через Telethon и складывает сырые сообщения в telegram_raw.

ВАЖНО про Telethon:
  - нужен .session файл. Первый вход интерактивный (ввод кода из телеграма),
    в headless-контейнере это не сработает. Сгенерируй сессию ЛОКАЛЬНО
    (см. README, "что доделать") и положи в ./telegram_session/, файл
    монтируется в контейнер.
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

from sqlalchemy import text

from airflow import DAG
from airflow.operators.python import PythonOperator

from common.config import telegram_channels, telegram_creds
from common.db import connection

# сколько последних сообщений тянуть с канала за один прогон
LIMIT_PER_CHANNEL = 50


async def _collect() -> list[dict]:
    from telethon import TelegramClient

    api_id, api_hash, session = telegram_creds()
    channels = telegram_channels()
    if not (api_id and api_hash and channels):
        print("[tg] не заданы TELEGRAM_API_ID/HASH/CHANNELS — пропуск")
        return []

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
    print(f"[tg] собрано сообщений: {len(rows)}")
    return rows


def ingest(**_):
    rows = asyncio.run(_collect())
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
