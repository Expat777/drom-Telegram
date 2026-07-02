"""
LLM-фолбэк для разбора «сложных» телеграм-текстов, где regex не справился.

Работает через OpenAI-совместимый эндпоинт (LLM_BASE_URL / LLM_API_KEY /
LLM_MODEL из .env). Спроектирован так, чтобы быть НЕОБЯЗАТЕЛЬНЫМ:
  - если LLM_API_KEY не задан — возвращает {} (LLM просто выключен);
  - при любой ошибке (сеть, таймаут, кривой JSON) — возвращает {} и логирует,
    не роняя нормализацию.

Ожидаемый ответ модели — JSON с частью полей единого вида:
    {"brand","model","year","price","mileage","region"}
"""
from __future__ import annotations

import json
import re

import requests

from common.config import llm_config

_TIMEOUT = 25
_ALLOWED = {"brand", "model", "year", "price", "mileage", "region"}

_SYSTEM = (
    "Ты извлекаешь структурированные данные об автомобиле из текста объявления. "
    "Верни СТРОГО JSON без пояснений с полями: "
    "brand (марка, латиницей как принято), model, year (int), "
    "price (число, рубли), mileage (число, км), region (город). "
    "Если поля нет в тексте — ставь null."
)


def _coerce(data: dict) -> dict:
    """Оставляем только известные поля и приводим числа к нужным типам."""
    out: dict = {}
    for k in _ALLOWED:
        v = data.get(k)
        if v is None or v == "":
            continue
        if k == "year":
            try:
                out[k] = int(v)
            except (TypeError, ValueError):
                continue
        elif k in ("price", "mileage"):
            try:
                out[k] = float(v)
            except (TypeError, ValueError):
                continue
        else:
            out[k] = str(v).strip() or None
    return out


def _extract_json(content: str) -> dict:
    """Достаём JSON-объект из ответа модели (на случай обёртки в текст/```)."""
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", content, flags=re.DOTALL)
        return json.loads(m.group(0)) if m else {}


def parse_with_llm(text: str) -> dict:
    """Разобрать текст объявления LLM-ом. Возвращает {} если LLM выключен/ошибка."""
    if not text or not text.strip():
        return {}
    api_key, base_url, model = llm_config()
    if not api_key or not base_url:
        return {}   # LLM не настроен — тихо выключен

    url = base_url.rstrip("/") + "/chat/completions"
    payload = {
        "model": model,
        "temperature": 0,
        "messages": [
            {"role": "system", "content": _SYSTEM},
            {"role": "user", "content": text[:4000]},
        ],
    }
    try:
        resp = requests.post(
            url,
            headers={"Authorization": f"Bearer {api_key}",
                     "Content-Type": "application/json"},
            json=payload,
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        return _coerce(_extract_json(content))
    except Exception as e:  # noqa: BLE001
        print(f"[llm] фолбэк не сработал: {e}")
        return {}
