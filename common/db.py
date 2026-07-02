"""Работа с Postgres через SQLAlchemy engine."""
from contextlib import contextmanager

from sqlalchemy import create_engine, text

from common.config import db_dsn

_engine = None


def get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(db_dsn(), pool_pre_ping=True, future=True)
    return _engine


@contextmanager
def connection():
    eng = get_engine()
    with eng.begin() as conn:
        yield conn


def fetch_df(sql: str, params: dict | None = None):
    """Читает SELECT в pandas.DataFrame."""
    import pandas as pd

    return pd.read_sql_query(sql, get_engine(), params=params or None)
