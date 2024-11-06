import requests
from airflow.hooks.base import BaseHook
from airflow.models import Variable


# Function to get the Airbyte connection ID
def get_airbyte_connection_id(connection_name):
    # Dynamically retrieve the token
    connection = BaseHook.get_connection('airbyte_api')
    # To understand why schema : https://airflow.apache.org/docs/apache-airflow-providers-airbyte/stable/_modules/airflow/providers/airbyte/hooks/airbyte.html
    token = Variable.get("airbyte_token")
    api_url = connection.host

    headers = {
        "accept": "application/json",
        "authorization": f'Bearer {token}'
    }

    # API call to get the list of connections
    # Be sure to use the public endpoint here : https://docs.airbyte.com/using-airbyte/configuring-api-access
    response = requests.get(f"{api_url}connections", headers=headers)

    if response.status_code == 200:
        connections = response.json().get('data', [])
        for conn in connections:
            if conn['name'] == connection_name:
                return conn['connectionId']
    raise Exception(f"Connection not found with response: {response.json()} ")