"""
DAG normalize_car_listings — приводит drom_raw и telegram_raw к единому виду
и складывает в car_listings (upsert по url).

Использует common.parsers — ТОТ ЖЕ код, что и сервис предсказания.
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import text

from airflow import DAG
from airflow.operators.python import PythonOperator

from common.db import connection, get_engine
from common.parsers import normalize_drom, normalize_telegram

UPSERT = text("""
    INSERT INTO car_listings
        (brand, model, year, price, mileage, region, source, url, collected_at)
    VALUES
        (:brand, :model, :year, :price, :mileage, :region, :source, :url, :collected_at)
    ON CONFLICT (url) DO UPDATE SET
        brand = EXCLUDED.brand, model = EXCLUDED.model, year = EXCLUDED.year,
        price = EXCLUDED.price, mileage = EXCLUDED.mileage,
        region = EXCLUDED.region, collected_at = EXCLUDED.collected_at
""")


def _load_raw(table: str) -> list[dict]:
    import pandas as pd

    with get_engine().connect() as conn:
        df = pd.read_sql(text(f"SELECT * FROM {table}"), conn)
    return df.to_dict("records")


def normalize(**_):
    inserted = 0
    with connection() as conn:
        # drom
        for row in _load_raw("drom_raw"):
            raw = row.get("raw") or {}
            if isinstance(raw, str):
                import json
                raw = json.loads(raw)
            raw.setdefault("url", row.get("url"))
            unified = normalize_drom(raw)
            if _is_valid(unified):
                conn.execute(UPSERT, unified)
                inserted += 1

        # telegram
        for row in _load_raw("telegram_raw"):
            unified = normalize_telegram({
                "text": row.get("text"),
                "url": row.get("url"),
                "channel": row.get("channel"),
            })
            if _is_valid(unified):
                conn.execute(UPSERT, unified)
                inserted += 1
    print(f"[normalize] upsert строк: {inserted}")


def _is_valid(u: dict) -> bool:
    """Минимальный фильтр мусора: нужна ссылка и хотя бы цена или марка."""
    return bool(u.get("url")) and (u.get("price") is not None
                                   or u.get("brand") is not None)


with DAG(
    dag_id="normalize_car_listings",
    start_date=datetime(2024, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    tags=["normalize"],
) as dag:
    PythonOperator(task_id="normalize", python_callable=normalize)
