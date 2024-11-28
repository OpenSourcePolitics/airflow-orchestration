FROM apache/airflow:2.10.1-python3.10

# Repasser à l'utilisateur airflow
USER airflow

# Installer les providers nécessaires pour Airflow
RUN pip install apache-airflow-providers-airbyte[http] \
&& pip install apache-airflow-providers-airbyte==4.0.0 \
&& pip install apache-airflow-providers-github \
&& pip install grist_api
