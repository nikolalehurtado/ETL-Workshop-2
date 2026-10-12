# Imagen de Airflow del proyecto: oficial 3.1.8 + Great Expectations.
# Instalar las dependencias en la imagen (y no al arrancar cada contenedor)
# hace el entorno reproducible y el arranque mas rapido.
FROM apache/airflow:3.1.8

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==3.1.8" -r /requirements.txt
