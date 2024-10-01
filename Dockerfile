FROM apache/airflow:2.10.1-python3.10

# Repasser à l'utilisateur airflow
USER airflow

# Copier le fichier requirements
COPY requirements.txt /tmp/

# Installer les providers nécessaires pour Airflow
RUN pip install apache-airflow-providers-airbyte[http] \
&& pip install apache-airflow-providers-airbyte \
&& pip install apache-airflow-providers-github

# Copier les DAGs avec la bonne propriété
COPY --chown=airflow:root /dags /opt/airflow/dags
