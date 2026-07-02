-- Создаём отдельные БД для метаданных Airflow и Superset.
-- Данные приложения (таблицы ниже) лежат в основной БД POSTGRES_DB (cars).
CREATE DATABASE airflow;
CREATE DATABASE superset;
