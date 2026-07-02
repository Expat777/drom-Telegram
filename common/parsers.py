"""
Общий парсер для обоих источников.

Этот модуль импортируют И DAG нормализации (шаг 2), И сервис предсказания
(шаг 4) — так выполняется требование "сервис парсит ссылку тем же кодом".

Функции верхнего уровня:
  - normalize_drom(raw)      -> dict  единого вида
  - normalize_telegram(raw)  -> dict  единого вида
  - parse_listing_url(url)   -> dict  (для сервиса предсказания)

Единый вид (unified dict):
  brand, model, year, price, mileage, region, source, url, collected_at
"""
from __future__ import annotations

import re
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

# ----------------------------------------------------------------------
# Список марок для распознавания в свободном тексте (телеграм).
# Дополняй по мере необходимости.
# ----------------------------------------------------------------------
BRANDS = [
    "Toyota", "Honda", "Nissan", "Mazda", "Mitsubishi", "Subaru", "Suzuki",
    "Lexus", "Hyundai", "Kia", "Volkswagen", "Audi", "BMW", "Mercedes",
    "Mercedes-Benz", "Skoda", "Renault", "Ford", "Chevrolet", "Opel",
    "Lada", "ВАЗ", "Geely", "Chery", "Haval", "Changan", "Volvo", "Peugeot",
    "Citroen", "Land Rover", "Porsche", "Infiniti", "Datsun", "UAZ", "УАЗ",
]

_UNIFIED_KEYS = ("brand", "model", "year", "price", "mileage",
                 "region", "source", "url", "collected_at")

# Сокращения/кириллические написания марок -> каноничное имя из BRANDS.
# Без этого "VW" и "Volkswagen" были бы разными значениями в дашборде.
BRAND_ALIASES = {
    "vw": "Volkswagen",
    "фольксваген": "Volkswagen",
    "мерседес": "Mercedes-Benz",
    "мерс": "Mercedes-Benz",
    "бмв": "BMW",
    "тойота": "Toyota",
    "ниссан": "Nissan",
    "хендай": "Hyundai",
    "хёндай": "Hyundai",
    "киа": "Kia",
    "шкода": "Skoda",
    "лада": "Lada",
    "рено": "Renault",
}

# Крупные города/регионы для распознавания в свободном тексте (телеграм).
# Ключ — каноничное имя в БД, значения — варианты написания/склонения в тексте.
REGIONS = {
    "Москва": ["москва", "москве", "мск"],
    "Санкт-Петербург": ["санкт-петербург", "спб", "питер", "петербург"],
    "Новосибирск": ["новосибирск"],
    "Екатеринбург": ["екатеринбург", "екб"],
    "Казань": ["казань", "казани"],
    "Нижний Новгород": ["нижний новгород", "нижнем новгороде", "нижнем"],
    "Челябинск": ["челябинск"],
    "Самара": ["самара", "самаре"],
    "Омск": ["омск", "омске"],
    "Ростов-на-Дону": ["ростов-на-дону", "ростов", "ростове"],
    "Уфа": ["уфа", "уфе"],
    "Красноярск": ["красноярск", "красноярске"],
    "Воронеж": ["воронеж", "воронеже"],
    "Пермь": ["пермь", "перми"],
    "Волгоград": ["волгоград", "волгограде"],
    "Краснодар": ["краснодар", "краснодаре"],
    "Владивосток": ["владивосток", "владивостоке", "владик"],
    "Хабаровск": ["хабаровск", "хабаровске"],
    "Иркутск": ["иркутск", "иркутске"],
    "Тюмень": ["тюмень", "тюмени"],
}


# ----------------------------------------------------------------------
# Низкоуровневые извлекалки из текста (общие для drom и telegram)
# ----------------------------------------------------------------------
def extract_year(text: str) -> int | None:
    """Год выпуска: 4 цифры в диапазоне 1950..текущий+1."""
    now = datetime.now().year
    for m in re.finditer(r"\b(19[5-9]\d|20[0-4]\d)\b", text):
        y = int(m.group(1))
        if 1950 <= y <= now + 1:
            return y
    return None


def extract_price(text: str) -> float | None:
    """
    Цена в рублях. Понимает '1 200 000', '1200000 руб', '1.2 млн', '950 т.р.'.
    """
    t = text.lower().replace(" ", " ")

    # 1.2 млн / 1,2 миллиона
    m = re.search(r"(\d+[.,]?\d*)\s*(?:млн|миллион)", t)
    if m:
        return float(m.group(1).replace(",", ".")) * 1_000_000

    # 950 тыс / 950 т.р. / 950 к
    m = re.search(r"(\d+[.,]?\d*)\s*(?:тыс|т\.?\s*р|к\b)", t)
    if m:
        return float(m.group(1).replace(",", ".")) * 1_000

    # обычное число рядом со словом цена/руб/₽ или просто крупное число
    candidates = re.findall(r"(\d[\d\s]{4,})\s*(?:руб|₽|р\.)", t)
    if not candidates:
        m = re.search(r"цена[:\s]*([\d\s]{5,})", t)
        if m:
            candidates = [m.group(1)]
    for c in candidates:
        val = float(re.sub(r"\s", "", c))
        if 30_000 <= val <= 100_000_000:
            return val
    return None


def extract_mileage(text: str) -> float | None:
    """Пробег в км: 'пробег 120 000', '120000 км', '120 тыс км'."""
    t = text.lower().replace(" ", " ")

    m = re.search(r"пробег[:\s]*([\d\s]{2,})\s*(?:тыс)?\s*км?", t)
    if not m:
        m = re.search(r"([\d\s]{3,})\s*км\b", t)
    if not m:
        m = re.search(r"([\d\s]{2,})\s*тыс\.?\s*км", t)
    if m:
        val = float(re.sub(r"\s", "", m.group(1)))
        if "тыс" in t[m.start():m.end() + 6] and val < 1000:
            val *= 1000
        if 0 < val <= 1_000_000:
            return val
    return None


def extract_region(text: str) -> str | None:
    """Ищем известный город/регион в свободном тексте объявления."""
    t = text.lower()
    for canonical, variants in REGIONS.items():
        for v in variants:
            if re.search(rf"\b{re.escape(v)}\b", t):
                return canonical
    return None


def extract_brand_model(text: str) -> tuple[str | None, str | None]:
    """Ищем марку из BRANDS или алиасов (VW->Volkswagen), модель — слово после."""
    # (искомый_термин, каноничная_марка); длинные термины проверяем первыми,
    # чтобы "Mercedes-Benz" сматчился раньше "мерс".
    terms = [(b, b) for b in BRANDS] + list(BRAND_ALIASES.items())
    for term, canonical in sorted(terms, key=lambda p: len(p[0]), reverse=True):
        m = re.search(rf"\b{re.escape(term)}\b\s*([A-Za-zА-Яа-я0-9\-]+)?",
                      text, flags=re.IGNORECASE)
        if m:
            model = m.group(1)
            # отсекаем мусорные "модели"
            if model and model.lower() in {"года", "год", "за", "в", "с"}:
                model = None
            return canonical, model
    return None, None


# ----------------------------------------------------------------------
# Нормализация raw-строк из БД в единый вид
# ----------------------------------------------------------------------
def _empty_unified(source: str, url: str | None) -> dict:
    return {
        "brand": None, "model": None, "year": None, "price": None,
        "mileage": None, "region": None, "source": source, "url": url,
        "collected_at": datetime.now(timezone.utc),
    }


def drom_url_parts(url: str | None) -> tuple[str | None, str | None, str | None]:
    """Из URL drom достаём (city, brand, model): .../<city>/<brand>/<model>/<id>.html"""
    if not url:
        return None, None, None
    m = re.search(r"drom\.ru/([^/]+)/([^/]+)/([^/]+)/\d+\.html", url)
    if not m:
        return None, None, None
    city, brand, model = m.groups()
    return city, brand, model


def _slug_to_name(slug: str | None) -> str | None:
    if not slug:
        return None
    return slug.replace("_", " ").replace("-", " ").title()


# Латинские slug'и городов из URL drom -> те же русские названия, что и в REGIONS
# (иначе регион drom "Moscow" и telegram "Москва" — разные значения в дашборде).
DROM_CITY_TO_REGION = {
    "moscow": "Москва",
    "spb": "Санкт-Петербург",
    "sankt-peterburg": "Санкт-Петербург",
    "novosibirsk": "Новосибирск",
    "ekaterinburg": "Екатеринбург",
    "kazan": "Казань",
    "nizhny_novgorod": "Нижний Новгород",
    "chelyabinsk": "Челябинск",
    "samara": "Самара",
    "omsk": "Омск",
    "rostov-na-donu": "Ростов-на-Дону",
    "ufa": "Уфа",
    "krasnoyarsk": "Красноярск",
    "voronezh": "Воронеж",
    "perm": "Пермь",
    "volgograd": "Волгоград",
    "krasnodar": "Краснодар",
    "vladivostok": "Владивосток",
    "khabarovsk": "Хабаровск",
    "irkutsk": "Иркутск",
    "tyumen": "Тюмень",
}


def _drom_region(city_slug: str | None) -> str | None:
    """Русское название региона по slug'у из URL drom; иначе — из slug'а как есть."""
    if not city_slug:
        return None
    return DROM_CITY_TO_REGION.get(city_slug.lower()) or _slug_to_name(city_slug)


def normalize_drom(raw: dict) -> dict:
    """
    raw — то, что drom_ingest положил в drom_raw.raw:
    {url, title '<Марка> <Модель>, <год>', price_text, desc_items[...]}.
    Марку/модель/город берём из URL (латиница, надёжнее), остальное — из карточки.
    """
    url = raw.get("url")
    out = _empty_unified("drom", url)
    city, brand, model = drom_url_parts(url)
    out["brand"] = _slug_to_name(brand) or raw.get("brand")
    out["model"] = _slug_to_name(model) or raw.get("model")
    out["region"] = _drom_region(city) or raw.get("region")

    title = raw.get("title") or ""
    out["year"] = _to_int(raw.get("year")) or extract_year(title)

    # цена: из числа, либо из price_text ("1 195 000" — заведомо цена, парсим цифры)
    price = _to_float(raw.get("price"))
    if price is None and raw.get("price_text"):
        digits = re.sub(r"[^\d]", "", raw["price_text"])
        price = float(digits) if digits else None
    out["price"] = price

    # пробег — среди desc_items ищем элемент с "км"
    mileage = raw.get("mileage")
    if mileage is None:
        for item in raw.get("desc_items") or []:
            if "км" in item.lower():
                mileage = extract_mileage(item)
                break
    out["mileage"] = _to_float(mileage)
    return out


def normalize_telegram(raw: dict) -> dict:
    """
    raw — строка из telegram_raw (нужны поля 'text', 'url', 'channel').
    Тащим поля из свободного текста регулярками. Если regex не справился и
    настроен LLM — добираем LLM-ом (см. TODO ниже).
    """
    text = raw.get("text") or ""
    out = _empty_unified("telegram", raw.get("url"))
    brand, model = extract_brand_model(text)
    out["brand"] = brand
    out["model"] = model
    out["year"] = extract_year(text)
    out["price"] = extract_price(text)
    out["mileage"] = extract_mileage(text)
    # регион: сперва из текста, иначе — гео из raw (если проставлено выше)
    out["region"] = extract_region(text) or raw.get("region")

    # LLM-фолбэк: если regex не вытащил ключевые поля, а LLM настроен —
    # добираем модель-ом. parse_with_llm сам возвращает {} при выключенном LLM
    # или любой ошибке, поэтому это безопасно и не роняет нормализацию.
    if out["price"] is None or out["brand"] is None:
        from common.llm import parse_with_llm

        llm = parse_with_llm(text)
        for k, v in llm.items():
            if k in _UNIFIED_KEYS and out.get(k) is None and v is not None:
                out[k] = v
    return out


# ----------------------------------------------------------------------
# Для сервиса предсказания: по ссылке вернуть признаки
# ----------------------------------------------------------------------
def parse_listing_url(url: str) -> dict:
    """
    Определяет источник по URL, скачивает страницу/пост и возвращает единый dict.
    Используется ТЕМ ЖЕ кодом, что и нормализация (см. README, шаг 4).
    """
    if "drom.ru" in url:
        return normalize_drom(_fetch_drom(url))
    if "t.me" in url or "telegram" in url:
        return normalize_telegram(_fetch_telegram_post(url))
    raise ValueError(f"Неизвестный источник ссылки: {url}")


_DROM_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
}


def _fetch_drom(url: str) -> dict:
    """
    Скачивает страницу объявления drom.ru и вытаскивает поля.
    Марку/модель/город берём из URL, год — из заголовка/текста.

    Цену и пробег берём из структурных полей самой карточки объявления
    ([data-ftid="bulletin-price"] / [data-ftid="specification-mileage"]), а
    НЕ регуляркой по всему тексту страницы — там попадаются посторонние цифры
    (например, цены в блоке "похожие запчасти"), которые перебивают реальную
    цену авто.

    Важно: страницы в windows-1251.
    """
    resp = requests.get(url, timeout=25, headers=_DROM_HEADERS)
    resp.raise_for_status()
    resp.encoding = "windows-1251"
    soup = BeautifulSoup(resp.text, "html.parser")
    page_text = soup.get_text(" ", strip=True)

    city, brand, model = drom_url_parts(url)
    h1 = soup.find("h1")
    title = h1.get_text(" ", strip=True) if h1 else page_text[:120]

    price_el = soup.select_one('[data-ftid="bulletin-price"]')
    price = extract_price(price_el.get_text(" ", strip=True)) if price_el else None

    mileage_el = soup.select_one('[data-ftid="specification-mileage"]')
    mileage = extract_mileage(mileage_el.get_text(" ", strip=True)) if mileage_el else None

    return {
        "url": url,
        "brand": _slug_to_name(brand),
        "model": _slug_to_name(model),
        "region": _slug_to_name(city),
        "year": extract_year(title) or extract_year(page_text),
        "price": price if price is not None else extract_price(page_text),
        "mileage": mileage if mileage is not None else extract_mileage(page_text),
    }


def _fetch_telegram_post(url: str) -> dict:
    """
    Одиночный пост t.me/<channel>/<id>. Публичные посты доступны как
    веб-превью t.me/s/<channel>/<id> без авторизации.
    """
    m = re.search(r"t\.me/(?:s/)?([^/]+)/(\d+)", url)
    if not m:
        return {"url": url, "text": ""}
    channel, msg_id = m.group(1), m.group(2)
    preview = f"https://t.me/s/{channel}/{msg_id}"
    from common.config import telegram_proxy
    resp = requests.get(preview, timeout=20,
                        headers={"User-Agent": "Mozilla/5.0"},
                        proxies=telegram_proxy())
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")
    node = soup.select_one(".tgme_widget_message_text")
    return {"url": url, "channel": channel,
            "text": node.get_text(" ", strip=True) if node else ""}


# ----------------------------------------------------------------------
def _to_int(v):
    try:
        return int(float(str(v).replace(" ", "").replace(" ", "")))
    except (TypeError, ValueError):
        return None


def _to_float(v):
    try:
        return float(re.sub(r"[^\d.]", "", str(v).replace(",", ".")))
    except (TypeError, ValueError):
        return None
