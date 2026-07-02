# Car price platform (drom.ru + Telegram)

Сбор объявлений → нормализация в общую таблицу → дашборд в Superset →
сервис предсказания цены по ссылке. Весь стек в docker-compose.

## Состав стека
| Сервис          | Порт | Назначение |
|-----------------|------|------------|
| postgres        | 5432 | БД: `cars` (данные) + `airflow`, `superset` (метаданные) |
| airflow         | 8080 | DAG'и сбора/нормализации/обучения |
| superset        | 8088 | дашборд по `car_listings` |
| prediction-api  | 8010 | ссылка → предсказанная цена (внутри контейнера — 8000, см. `PREDICTION_PORT`) |

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

### 2. Telegram-сессия (сделать ДО первого прогона telegram_ingest)
Telethon требует интерактивного логина — делаем локально, потом кладём файл на сервер:
```bash
# на локальной машине:
pip install telethon
TELEGRAM_API_ID=... TELEGRAM_API_HASH=... python scripts/gen_telegram_session.py
# получится telegram_session/ingest.session — скопировать на сервер:
scp telegram_session/ingest.session deploy@<server>:~/car-price/project_drom_teleg/telegram_session/
```

### 3. Первый прогон DAG'ов (набрать датасет)
Открыть Airflow `http://<server>:8080` (логин/пароль из `.env`), включить и запустить:
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
- **Superset:** `http://<server>:8088` → Settings → Database Connections → добавить
  `postgresql+psycopg2://<POSTGRES_USER>:<POSTGRES_PASSWORD>@postgres:5432/cars`,
  затем Dataset `car_listings` → собрать чарты (средняя цена по маркам, динамика по
  регионам, drom vs telegram) → дашборд.
- **Сервис:** форма на `http://<server>:8010/` или API:
```bash
curl -X POST http://<server>:8010/predict \
  -H "Content-Type: application/json" \
  -d '{"url":"https://auto.drom.ru/..."}'
```

Есть также nginx (сервис `nginx` в `docker-compose.yml`) с двумя «внешними» точками
входа: `http://<server>:80` → Superset, `http://<server>:81` → сервис предсказания.
Прямые порты 8080/8088/8010 пока тоже открыты (нужны для Airflow UI и для отладки) —
закрывать их фаерволом будем вместе, когда закончим активную разработку (см. TODO #5).

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

## ⚠️ Что ещё НАДО ДОДЕЛАТЬ (каркас, не готовый парсинг)

1. **Парсер drom (`dags/drom_ingest.py` + `common/parsers.py::_fetch_drom`)** —
   селекторы рабочие (проверено реальным прогоном на сервере 2026-07-02 —
   собрано 40 карточек, нормализовано 20 строк в `car_listings`). Открытый
   риск — антибот drom при частом опросе (раз в 10 минут): нет ретраев,
   задержек между запросами и ротации User-Agent. Если начнутся 403/капча —
   добавить это в `scrape_drom()`.
2. **Регион в telegram** — сейчас пусто; можно проставлять по гео канала или
   парсить из текста.
3. **LLM для сложных телеграм-текстов** — заглушка в `normalize_telegram`
   (регулярки уже работают для простых случаев). При желании подключить —
   есть готовый агент в `../project/project` (DeepSeek).
4. **Проверить обучение** — GradientBoosting взят по умолчанию; после набора
   данных посмотреть метрики, при желании поменять модель/фичи.
5. **Прод-доступ** — nginx перед Superset (:80) и `/predict` (:81) уже поднят
   (сервис `nginx`). Осталось закрыть фаерволом прямые порты 8080/8088/8010 —
   не делаем это в одиночку, пока кто-то активно ходит в Airflow UI напрямую;
   закрываем вместе на финальном шаге.
```
