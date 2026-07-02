# Car price platform (drom.ru + Telegram)

Сбор объявлений → нормализация в общую таблицу → дашборд в Superset →
сервис предсказания цены по ссылке. Весь стек в docker-compose.

Это краткая справка (что где лежит, как поднять, куда смотреть). Подробный
разбор — как всё устроено технически, почему именно так, и пошаговое
воспроизведение с нуля (включая все грабли, на которые мы наступили) — в
[GUIDE.md](GUIDE.md).

## Состав стека
| Сервис          | Порт наружу | Порт localhost-only (SSH-туннель) | Назначение |
|-----------------|-------------|-----------------------------------|------------|
| nginx           | 80, 81      | —                                  | точки входа: 80 → Superset, 81 → prediction-api |
| postgres        | —           | 5432                               | БД: `cars` (данные) + `airflow`, `superset` (метаданные) |
| airflow         | —           | 8080                               | DAG'и сбора/нормализации/обучения |
| superset        | 80 (nginx)  | 8088                               | дашборд по `car_listings` |
| prediction-api  | 81 (nginx)  | 8010                               | ссылка → предсказанная цена (внутри контейнера — 8000) |

DAG'и: `drom_ingest`, `telegram_ingest`, `normalize_car_listings`, `train_price_model`.
Таблицы: `drom_raw`, `telegram_raw`, `car_listings`.

## Структура
```
docker-compose.yml        весь стек
airflow/                  образ airflow + зависимости
dags/                     4 DAG'а
common/                   ОБЩИЙ код: parsers.py используется и DAG'ом, и сервисом
prediction_service/       FastAPI + train.py
superset/                 конфиг + init
sql/                      создание БД и таблиц (init postgres)
scripts/                  генерация telegram-сессии (локально)
model_store/              сюда падает обученная модель (volume)
telegram_session/         .session для Telethon (volume)
nginx/                    конфиг реверс-прокси (2 точки входа наружу)
xray/                     конфиг SOCKS5-прокси для Telegram (config.json — не в git)
```

---

## Деплой на сервере (по шагам)

### 0. Что нужно на сервере
- Docker + docker compose plugin
- git

```bash
sudo apt update && sudo apt install -y docker.io docker-compose-plugin git
sudo systemctl enable --now docker
sudo usermod -aG docker $USER   # перелогиниться после этого
```

### 1. Как поднять весь стек
```bash
git clone <repo-url> car-price && cd car-price/project_drom_teleg
cp .env.example .env
# заполнить .env: пароли, ключи, telegram api_id/hash, каналы
#   AIRFLOW_FERNET_KEY:  python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
#   *_SECRET_KEY:        python -c "import secrets; print(secrets.token_hex(32))"
#   AIRFLOW_UID:         id -u   (на Linux)

docker compose up -d --build
docker compose ps
```

### 2. Telegram: каналы, LLM-ключ, (опционально) Telethon и прокси

**Обязательно:** заполнить `TELEGRAM_CHANNELS` в `.env` (публичные каналы, без `@`,
через запятую) и `LLM_API_KEY`/`LLM_BASE_URL`/`LLM_MODEL` — извлечение полей
(марка/модель/цена/пробег) из свободного текста постов идёт **в первую очередь
через LLM** (regex — только запасной вариант, если LLM не настроен или упал).
Без LLM-ключа тоже поедет, но качество извлечения будет хуже.

**Опционально — Telethon** (полная история, приватные каналы; по умолчанию не
нужен — без него `telegram_ingest` сам использует публичное веб-превью `t.me/s/<канал>`):
```bash
# на локальной машине:
pip install telethon
TELEGRAM_API_ID=... TELEGRAM_API_HASH=... python scripts/gen_telegram_session.py
# получится telegram_session/ingest.session — скопировать на сервер:
scp telegram_session/ingest.session deploy@<server>:~/car-price/project_drom_teleg/telegram_session/
```

**Если у хостера сервера заблокирован Telegram** (проверить: `curl https://t.me` —
если зависает/таймаутит при рабочем интернете в остальном, это оно — подробности
и как это выглядело у нас см. GUIDE.md): поднимается SOCKS5-прокси через сервис
`xray` (VLESS+Reality). Нужен свой VLESS-конфиг (например, из подписки любого
VPN-сервиса на этом протоколе):
```bash
cp xray/config.json.example xray/config.json
# вписать address/port/id/publicKey/serverName из своей VLESS-подписки
# в .env: TELEGRAM_PROXY=socks5h://xray:1080
docker compose up -d xray
```
`xray/config.json` в git не коммитим (личные данные подписки).

### 3. Первый прогон DAG'ов (набрать датасет)
Открыть Airflow (порт localhost-only, см. таблицу выше — с рабочей машины через
`ssh -L 8080:localhost:8080 <server>`, дальше `http://localhost:8080`), включить и запустить:
1. `drom_ingest` и `telegram_ingest` — дождаться данных в `drom_raw` / `telegram_raw`
2. `normalize_car_listings` — заполнит `car_listings`

Через CLI то же самое:
```bash
docker compose exec airflow-scheduler airflow dags unpause drom_ingest
docker compose exec airflow-scheduler airflow dags trigger drom_ingest
docker compose exec airflow-scheduler airflow dags trigger normalize_car_listings
```

### 4. Обучить модель
Когда в `car_listings` накопилось ≥ 30 строк:
```bash
# вариант А — через DAG:
docker compose exec airflow-scheduler airflow dags trigger train_price_model
# вариант Б — вручную:
docker compose exec prediction-api python train.py
```
Модель сохраняется в `model_store/price_model.joblib` (volume), сервис подхватывает её сам.

### 5. Дашборд и проверка сервиса
Две внешние точки входа — через nginx:
- **Superset:** `http://<server>/` (порт 80) → Settings → Database Connections → добавить
  `postgresql+psycopg2://<POSTGRES_USER>:<POSTGRES_PASSWORD>@postgres:5432/cars`,
  затем Dataset `car_listings` → собрать чарты (средняя цена по маркам, динамика по
  регионам, drom vs telegram) → дашборд.
- **Сервис:** форма на `http://<server>:81/` или API:
```bash
curl -X POST http://<server>:81/predict \
  -H "Content-Type: application/json" \
  -d '{"url":"https://auto.drom.ru/..."}'
```

Postgres/Airflow UI/сервис предсказания напрямую (5432/8080/8010) наружу закрыты —
привязаны к `127.0.0.1` на сервере. Доступ для админки — через SSH-туннель:
```bash
ssh -L 8080:localhost:8080 -L 5432:localhost:5432 <server>
# после этого http://localhost:8080 — Airflow UI с твоей машины
```

### 6. Логи
```bash
docker compose logs -f airflow-scheduler
docker compose logs -f prediction-api
docker compose logs -f superset
# логи конкретных тасков — в Airflow UI
```

### Обновление кода на сервере
```bash
./deploy.sh          # git pull origin master + docker compose up -d --build
```

---

## Статус: все пункты README закрыты и проверены на сервере

`car_listings` растёт непрерывно сама (DAG'и на расписании — раз в 10/15 минут),
дашборд в Superset собран из 6 чартов на реальных данных, `/predict` проверен
на живых ссылках drom и telegram. Модель обучается на `log(цена)` (см. GUIDE.md
зачем) — при широком разбросе цен ориентируйтесь на MedAE/MAPE, а не только на
MAE/RMSE (последние сильно шатаются от пары редких дорогих машин в тестовой
выборке — это нормально, не баг).

Известные ограничения (не блокирующие, на будущее):
1. **Антибот drom** — есть ретраи/бэкофф/ротация User-Agent/задержки
   (`scrape_drom()`), но при резком увеличении частоты/числа городов
   всё равно может словить 429 — если начнётся, сначала смотреть логи таска
   (см. GUIDE.md, раздел про массовый досбор).
2. **LLM — основной способ разбора telegram-постов**, regex — запасной (если
   `LLM_API_KEY` пуст или запрос упал). Без LLM-ключа качество извлечения
   марки/цены из вольного текста будет заметно хуже.
3. **Прод-доступ** — nginx перед Superset (:80) и `/predict` (:81), прямые
   порты (5432/8080/8088/8010) привязаны к `127.0.0.1` — доступ только через
   SSH-туннель с сервера, наружу не торчат. Для постоянного доступа к Airflow
   держите туннель через `autossh` (см. GUIDE.md) — падает реже, чем голый `ssh -L`.
