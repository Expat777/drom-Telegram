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
