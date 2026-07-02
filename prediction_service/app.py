"""
Сервис предсказания цены.

Точки входа (README, "что должно быть на выходе"):
  GET  /            — простая HTML-форма: вставил ссылку -> получил цену
  POST /predict     — {"url": "..."} -> {predicted_price, features, ...}
  GET  /health      — проверка живости

Парсинг ссылки идёт ТЕМ ЖЕ кодом, что и нормализация (common.parsers).
"""
from __future__ import annotations

import os

import joblib
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from common.parsers import parse_listing_url

MODEL_PATH = os.environ.get("MODEL_PATH", "/opt/model_store/price_model.joblib")

app = FastAPI(title="Car price prediction")
_model = None


def get_model():
    global _model
    if _model is None:
        if not os.path.exists(MODEL_PATH):
            raise HTTPException(503, f"Модель не найдена: {MODEL_PATH}. "
                                     "Запусти DAG train_price_model или train.py.")
        _model = joblib.load(MODEL_PATH)
    return _model


class PredictRequest(BaseModel):
    url: str


@app.get("/health")
def health():
    return {"status": "ok", "model_loaded": os.path.exists(MODEL_PATH)}


@app.post("/predict")
def predict(req: PredictRequest):
    try:
        feats = parse_listing_url(req.url)
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"Не смог распарсить ссылку: {e}")

    X = pd.DataFrame([{
        "brand": feats.get("brand") or "unknown",
        "model": feats.get("model") or "unknown",
        "year": feats.get("year") or 0,
        "mileage": feats.get("mileage") or 0,
        "region": feats.get("region") or "unknown",
    }])
    pred = float(get_model().predict(X)[0])

    result = {"predicted_price": round(pred), "features": feats}
    # бонус: сравнить с реальной ценой из объявления
    real = feats.get("price")
    if real:
        diff = (real - pred) / pred * 100
        result["real_price"] = real
        result["verdict"] = (f"переоценено на {diff:.0f}%" if diff > 0
                             else f"недооценено на {abs(diff):.0f}%")
    return result


@app.get("/", response_class=HTMLResponse)
def index():
    return """
    <html><body style="font-family:sans-serif;max-width:640px;margin:40px auto">
      <h2>Оценка цены авто по ссылке</h2>
      <input id="u" style="width:100%;padding:8px" placeholder="https://auto.drom.ru/... или https://t.me/...">
      <button onclick="go()" style="margin-top:8px;padding:8px 16px">Оценить</button>
      <pre id="out" style="background:#f4f4f4;padding:12px;margin-top:16px"></pre>
      <script>
        async function go(){
          const url = document.getElementById('u').value;
          const r = await fetch('/predict', {method:'POST',
            headers:{'Content-Type':'application/json'},
            body: JSON.stringify({url})});
          document.getElementById('out').textContent =
            JSON.stringify(await r.json(), null, 2);
        }
      </script>
    </body></html>
    """
