FROM apache/airflow:2.10.1-python3.10
# Passer à l'utilisateur root pour installer des paquets et modifier /etc/hosts
USER root

# Mettre à jour les paquets et installer iputils-ping et nano
RUN apt-get update \
    && apt-get install -y iputils-ping nano
# Repasser à l'utilisateur airflow
USER airflow

# Copier le fichier requirements
COPY requirements.txt /tmp/

# Installer les providers nécessaires pour Airflow
RUN pip install apache-airflow-providers-airbyte[http] \
&& pip install apache-airflow-providers-airbyte==4.0.0 \
&& pip install apache-airflow-providers-github

# Copier les DAGs avec la bonne propriété
COPY --chown=airflow:root /dags /opt/airflow/dags
