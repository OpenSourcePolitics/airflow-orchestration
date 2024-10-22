import requests
from airflow.models import Variable
from airflow.hooks.base import BaseHook

# Function to dynamically retrieve the token via the Airbyte API. Token are short-lived, 3 minutes.
def get_airbyte_token():
    # Retrieve API connection details from Airflow connections
    connection = BaseHook.get_connection('airbyte_api')
    api_url = connection.host
    client_id = connection.login
    client_secret = connection.password

    # Request to get the token via Airbyte API
    token_url = f"{api_url}/v1/token"  # URL to get the token according to your API spec
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
    token = get_airbyte_token()

    # Retrieve API connection details from Airflow
    connection = BaseHook.get_connection('airbyte_api')
    api_url = connection.host

    # Get the Workspace ID from an Airflow variable
    workspace_id = Variable.get("workspace_id")

    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
    }

    data = {
        'workspaceId': f'{workspace_id}'
    }

    # API call to get the list of connections
    response = requests.post(f"{api_url}/v1/connections/list", headers=headers, json=data)

    if response.status_code == 200:
        connections = response.json().get('connections', [])
        for conn in connections:
            if conn['name'] == connection_name:
                return conn['connectionId']
    raise Exception("Connection not found")