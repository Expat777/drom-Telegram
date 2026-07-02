FROM apache/airflow:2.10.5

COPY requirements.txt /requirements.txt

# Ставим провайдер postgres строго по constraints, остальное — сверху.
# Если что-то конфликтует по версиям — двигай пакет в prediction_service.
RUN pip install --no-cache-dir \
      "apache-airflow-providers-postgres" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt" \
 && pip install --no-cache-dir -r /requirements.txt
