"""
Одноразовая генерация Telethon-сессии ЛОКАЛЬНО (на своей машине, интерактивно).
Потом файл ./telegram_session/ingest.session копируется на сервер в ту же папку.

Запуск локально:
    pip install telethon
    TELEGRAM_API_ID=... TELEGRAM_API_HASH=... python scripts/gen_telegram_session.py
Введёшь номер телефона и код из телеграма — создастся ingest.session.
"""
import os

from telethon.sync import TelegramClient

api_id = int(os.environ["TELEGRAM_API_ID"])
api_hash = os.environ["TELEGRAM_API_HASH"]

os.makedirs("telegram_session", exist_ok=True)
with TelegramClient("telegram_session/ingest", api_id, api_hash) as client:
    me = client.get_me()
    print(f"OK, залогинен как: {me.username or me.first_name}")
    print("Сессия сохранена в telegram_session/ingest.session")
