"""Единая точка чтения настроек из окружения."""
import os


def db_dsn() -> str:
    return os.environ.get(
        "CARS_DB_DSN",
        "postgresql://cars:change_me_postgres@localhost:5432/cars",
    )


def telegram_channels() -> list[str]:
    raw = os.environ.get("TELEGRAM_CHANNELS", "")
    return [c.strip().lstrip("@") for c in raw.split(",") if c.strip()]


def telegram_creds() -> tuple[int | None, str | None, str]:
    api_id = os.environ.get("TELEGRAM_API_ID")
    api_hash = os.environ.get("TELEGRAM_API_HASH")
    session = os.environ.get("TELEGRAM_SESSION", "/opt/telegram_session/ingest")
    return (int(api_id) if api_id else None, api_hash, session)


def model_path() -> str:
    return os.environ.get("MODEL_PATH", "/opt/model_store/price_model.joblib")


def llm_config() -> tuple[str | None, str | None, str]:
    """(api_key, base_url, model). Пустой api_key => LLM выключен."""
    api_key = os.environ.get("LLM_API_KEY") or None
    base_url = os.environ.get("LLM_BASE_URL") or None
    model = os.environ.get("LLM_MODEL", "gpt-4o-mini")
    return api_key, base_url, model


def telegram_proxy() -> dict[str, str] | None:
    """
    dict для requests(proxies=...) или None, если прокси не настроен.
    Нужен, если у хостера сервера заблокирован Telegram (см. .env.example,
    сервис `xray` в docker-compose.yml). Пример значения:
    socks5h://xray:1080
    """
    proxy = os.environ.get("TELEGRAM_PROXY") or None
    if not proxy:
        return None
    return {"http": proxy, "https": proxy}
