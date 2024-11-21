import requests
from airflow.hooks.base import BaseHook
from airflow.models import Variable

# Function to dynamically retrieve the token via the Airbyte API. Token are short-lived, 3 minutes.
def get_airbyte_token():
    # Retrieve API connection details from Airflow connections
    connection = BaseHook.get_connection('airbyte_api')
    api_url = connection.host
    client_id = connection.login
    client_secret = connection.password

    # Request to get the token via Airbyte API
    token_url = f"{api_url}/applications/token"  # URL to get the token according to your API spec
    token_data = {
        'client_id': client_id,
        'client_secret': client_secret
    }

    response = requests.post(token_url, json=token_data)

    if response.status_code == 200:
        token = response.json().get('access_token')
        return token
    else:
        raise Exception("Failed to retrieve token")

# Function to get the Airbyte connection ID
def get_airbyte_connection_id(connection_name):
    # Dynamically retrieve the token
    connection = BaseHook.get_connection('airbyte_api')
    # To understand why schema : https://airflow.apache.org/docs/apache-airflow-providers-airbyte/stable/_modules/airflow/providers/airbyte/hooks/airbyte.html
    token = get_airbyte_token()
    api_url = connection.host

    headers = {
        "accept": "application/json",
        "authorization": f'Bearer {token}'
    }

    # API call to get the list of connections
    # Be sure to use the public endpoint here : https://docs.airbyte.com/using-airbyte/configuring-api-access
    # Be sure to use the max limit of 100, otherwise, the connections might not be found
    response = requests.get(f"{api_url}connections?limit=100", headers=headers)

    if response.status_code == 200:
        connections = response.json().get('data', [])
        for conn in connections:
            if conn['name'] == connection_name:
                return conn['connectionId']
    raise Exception(f"Connection not found with response: {response.json()} ")