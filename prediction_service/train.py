"""
Ручное обучение модели (альтернатива DAG train_price_model).
Запуск:  docker compose exec prediction-api python train.py
"""
import os

import joblib
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sqlalchemy import create_engine

DSN = os.environ["CARS_DB_DSN"]
MODEL_PATH = os.environ.get("MODEL_PATH", "/opt/model_store/price_model.joblib")


def main():
    eng = create_engine(DSN)
    df = pd.read_sql_query("""
        SELECT brand, model, year, mileage, region, price
        FROM car_listings
        WHERE price IS NOT NULL AND brand IS NOT NULL
    """, eng)

    print(f"строк: {len(df)}")
    if len(df) < 30:
        raise SystemExit("Мало данных (нужно >= 30). Накопи датасет ingest-DAG'ами.")

    df = df.fillna({"model": "unknown", "region": "unknown",
                    "year": df["year"].median(), "mileage": df["mileage"].median()})
    X = df[["brand", "model", "year", "mileage", "region"]]
    y = df["price"]

    pre = ColumnTransformer([
        ("cat", OneHotEncoder(handle_unknown="ignore"), ["brand", "model", "region"]),
        ("num", "passthrough", ["year", "mileage"]),
    ])
    pipe = Pipeline([("pre", pre),
                     ("model", GradientBoostingRegressor(random_state=42))])
    pipe.fit(X, y)
    joblib.dump(pipe, MODEL_PATH)
    print(f"модель сохранена: {MODEL_PATH}")


if __name__ == "__main__":
    main()
