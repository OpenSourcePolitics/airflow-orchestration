FROM apache/airflow:2.10.1-python3.10

# Passer à l'utilisateur root pour installer les dépendances système
USER root

# Installer les dépendances système nécessaires pour MySQL
RUN apt-get update && apt-get install -y \
    default-libmysqlclient-dev \
    build-essential \
    && apt-get clean

# Repasser à l'utilisateur airflow
USER airflow

# Installer les providers nécessaires pour Airflow et MySQL
RUN pip install apache-airflow-providers-airbyte[http] \
    && pip install apache-airflow-providers-airbyte==4.0.0 \
    && pip install apache-airflow-providers-github \
    && pip install grist_api \
    && pip install mysql-connector-python