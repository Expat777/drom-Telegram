#!/usr/bin/env bash
# Инициализация Superset при старте контейнера: миграции, админ, роли.
set -e

pip install --no-cache-dir psycopg2-binary >/dev/null 2>&1 || true

superset db upgrade

superset fab create-admin \
  --username "${ADMIN_USER:-admin}" \
  --firstname admin --lastname admin \
  --email admin@example.com \
  --password "${ADMIN_PASSWORD:-admin}" || true

superset init

exec /usr/bin/run-server.sh
