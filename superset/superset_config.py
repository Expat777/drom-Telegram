import os

# Метаданные Superset храним в отдельной БД superset на том же postgres.
_user = os.environ.get("POSTGRES_USER", "cars")
_pass = os.environ.get("POSTGRES_PASSWORD", "change_me_postgres")
SQLALCHEMY_DATABASE_URI = f"postgresql+psycopg2://{_user}:{_pass}@postgres:5432/superset"

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "change_me_superset")

# Без Redis/Celery — для учебного стека достаточно.
FEATURE_FLAGS = {"ALERT_REPORTS": False}
WTF_CSRF_ENABLED = True
# Разрешаем встраивать дашборды по ссылке (для «точки входа» из README).
PUBLIC_ROLE_LIKE = "Gamma"
