#!/usr/bin/env bash
# Деплой на сервере: подтянуть master и пересобрать стек.
set -euo pipefail

cd "$(dirname "$0")"

echo ">> git pull origin master"
git pull origin master

echo ">> docker compose up -d --build"
docker compose up -d --build

echo ">> статус контейнеров:"
docker compose ps
