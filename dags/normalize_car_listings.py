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


def _drom_to_unified(row: dict) -> dict:
    raw = row.get("raw") or {}
    if isinstance(raw, str):
        import json
        raw = json.loads(raw)
    raw.setdefault("url", row.get("url"))
    return normalize_drom(raw)


def _telegram_to_unified(row: dict) -> dict:
    return normalize_telegram({
        "text": row.get("text"),
        "url": row.get("url"),
        "channel": row.get("channel"),
    })


def _upsert_one(conn, row: dict, to_unified) -> str:
    """Нормализует и апсертит одну строку под своим SAVEPOINT.

    Возвращает 'ok' / 'skip' / 'error'. Ошибка на одной строке (битый raw,
    неожиданный формат, сбой вставки) откатывает только её savepoint и не
    роняет весь DagRun.
    """
    try:
        unified = to_unified(row)
        if not _is_valid(unified):
            return "skip"
        with conn.begin_nested():   # SAVEPOINT на строку
            conn.execute(UPSERT, unified)
        return "ok"
    except Exception as e:  # noqa: BLE001
        print(f"[normalize] пропуск строки id={row.get('id')} "
              f"url={row.get('url')}: {e}")
        return "error"


def normalize(**_):
    inserted = errors = skipped = 0
    with connection() as conn:
        for row in _load_raw("drom_raw"):
            r = _upsert_one(conn, row, _drom_to_unified)
            inserted += r == "ok"
            errors += r == "error"
            skipped += r == "skip"

        for row in _load_raw("telegram_raw"):
            r = _upsert_one(conn, row, _telegram_to_unified)
            inserted += r == "ok"
            errors += r == "error"
            skipped += r == "skip"
    print(f"[normalize] upsert строк: {inserted}, "
          f"пропущено (мусор): {skipped}, ошибок: {errors}")


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
