"""
DAG train_price_model — раз в неделю переобучает модель цены по car_listings
и сохраняет .joblib в общий volume (MODEL_PATH), откуда его читает сервис.

Обучение вынесено в common-независимый код здесь же, чтобы DAG был автономным.
Тот же алгоритм лежит в prediction_service/train.py — можно запускать вручную.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from common.config import model_path
from common.db import fetch_df


def train(**_):
    import joblib
    import numpy as np
    from sklearn.compose import ColumnTransformer
    from sklearn.ensemble import GradientBoostingRegressor
    from sklearn.metrics import mean_absolute_error, mean_squared_error
    from sklearn.model_selection import train_test_split
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder

    df = fetch_df("""
        SELECT brand, model, year, mileage, region, price
        FROM car_listings
        WHERE price IS NOT NULL AND brand IS NOT NULL
    """)
    print(f"[train] строк для обучения: {len(df)}")
    if len(df) < 30:
        raise ValueError("Слишком мало данных для обучения (нужно >= 30). "
                         "Сначала накопи датасет ingest-DAG'ами.")

    df = df.fillna({"model": "unknown", "region": "unknown",
                    "year": df["year"].median(), "mileage": df["mileage"].median()})
    X = df[["brand", "model", "year", "mileage", "region"]]
    y = df["price"]

    cat = ["brand", "model", "region"]
    num = ["year", "mileage"]

    def make_pipeline():
        pre = ColumnTransformer([
            ("cat", OneHotEncoder(handle_unknown="ignore"), cat),
            ("num", "passthrough", num),
        ])
        return Pipeline([("pre", pre), ("model", GradientBoostingRegressor(random_state=42))])

    # отложенная выборка только для оценки качества — финальная модель ниже
    # обучается уже на всех данных
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    eval_pipe = make_pipeline()
    eval_pipe.fit(X_train, y_train)
    pred = eval_pipe.predict(X_test)
    mae = mean_absolute_error(y_test, pred)
    rmse = mean_squared_error(y_test, pred) ** 0.5
    mape = float(np.mean(np.abs((y_test - pred) / y_test))) * 100
    print(f"[train] метрики на отложенной выборке ({len(X_test)} строк): "
          f"MAE={mae:,.0f} руб, RMSE={rmse:,.0f} руб, MAPE={mape:.1f}%")

    pipe = make_pipeline()
    pipe.fit(X, y)

    path = model_path()
    joblib.dump(pipe, path)
    print(f"[train] модель сохранена: {path}")


with DAG(
    dag_id="train_price_model",
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",
    catchup=False,
    tags=["ml"],
) as dag:
    PythonOperator(task_id="train", python_callable=train)
