-- Таблицы приложения в БД cars.

-- Сырые данные с drom.ru (как есть, без обработки)
CREATE TABLE IF NOT EXISTS drom_raw (
    id           BIGSERIAL PRIMARY KEY,
    url          TEXT,
    raw          JSONB,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Сырые сообщения из телеграм-каналов
CREATE TABLE IF NOT EXISTS telegram_raw (
    id           BIGSERIAL PRIMARY KEY,
    channel      TEXT        NOT NULL,
    message_id   BIGINT      NOT NULL,
    text         TEXT,
    url          TEXT,
    raw          JSONB,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (channel, message_id)
);

-- Нормализованная общая таблица (источник для Superset и модели)
CREATE TABLE IF NOT EXISTS car_listings (
    id           BIGSERIAL PRIMARY KEY,
    brand        TEXT,
    model        TEXT,
    year         INTEGER,
    price        NUMERIC,
    mileage      NUMERIC,
    region       TEXT,
    source       TEXT NOT NULL,          -- 'drom' | 'telegram'
    url          TEXT UNIQUE,            -- для идемпотентного upsert
    collected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_car_listings_brand  ON car_listings (brand);
CREATE INDEX IF NOT EXISTS idx_car_listings_source ON car_listings (source);
CREATE INDEX IF NOT EXISTS idx_car_listings_region ON car_listings (region);
