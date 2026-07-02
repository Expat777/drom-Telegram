"""
Ручное обучение модели (альтернатива DAG train_price_model).
Запуск:  docker compose exec prediction-api python train.py
"""
import os

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer, TransformedTargetRegressor
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, median_absolute_error
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sqlalchemy import create_engine

DSN = os.environ["CARS_DB_DSN"]
MODEL_PATH = os.environ.get("MODEL_PATH", "/opt/model_store/price_model.joblib")

FEATURES = ["brand", "model", "year", "mileage", "region"]


def make_pipeline() -> TransformedTargetRegressor:
    """
    Цены на авто разбросаны на 3 порядка (десятки тысяч — десятки миллионов),
    поэтому обучаем регрессор на log1p(цена), а не на самой цене: иначе
    редкие дорогие машины перетягивают на себя всю ошибку модели, а дешёвые
    предсказываются плохо в относительных величинах. TransformedTargetRegressor
    делает лог/обратный-лог прозрачно — predict() как и раньше возвращает
    цену в рублях, /predict в app.py менять не нужно.
    """
    pre = ColumnTransformer([
        ("cat", OneHotEncoder(handle_unknown="ignore"), ["brand", "model", "region"]),
        ("num", "passthrough", ["year", "mileage"]),
    ])
    inner = Pipeline([("pre", pre), ("model", GradientBoostingRegressor(random_state=42))])
    return TransformedTargetRegressor(regressor=inner, func=np.log1p, inverse_func=np.expm1)


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
    X = df[FEATURES]
    y = df["price"]

    # отложенная выборка только для оценки качества — финальная модель ниже
    # обучается уже на всех данных
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    eval_pipe = make_pipeline()
    eval_pipe.fit(X_train, y_train)
    pred = eval_pipe.predict(X_test)
    mae = mean_absolute_error(y_test, pred)
    medae = median_absolute_error(y_test, pred)
    rmse = mean_squared_error(y_test, pred) ** 0.5
    mape = float(np.mean(np.abs((y_test - pred) / y_test))) * 100
    print(f"метрики на отложенной выборке ({len(X_test)} строк): "
          f"MAE={mae:,.0f} руб, MedAE={medae:,.0f} руб, RMSE={rmse:,.0f} руб, MAPE={mape:.1f}%")

    pipe = make_pipeline()
    pipe.fit(X, y)
    joblib.dump(pipe, MODEL_PATH)
    print(f"модель сохранена: {MODEL_PATH}")


if __name__ == "__main__":
    main()
