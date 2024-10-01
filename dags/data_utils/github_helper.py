from airflow.hooks.base import BaseHook
import time
from airflow.exceptions import AirflowException
from airflow.models import Variable
from github import Github


# Function to retrieve the GitHub token from the configured Airflow connection
def get_github_token(conn_id='github_connection'):
    # Retrieve the connection from Airflow Connections
    connection = BaseHook.get_connection(conn_id)

    # Access the token from the connection's password field
    token = connection.password
    return token


# Function to trigger the GitHub Action workflow
def trigger_workflow(workflow_file, **context):
    g = Github(context['params']['token'])  # Initialize the GitHub instance with the token from parameters
    repo_name = Variable.get("dbt_repository_name")
    repo = g.get_repo(repo_name)  # Access the repository
    workflow = repo.get_workflow(workflow_file)  # Select the workflow based on the provided YAML file name

    workflow.create_dispatch(ref='main')  # Trigger the workflow

    time.sleep(5)  # Wait for the run to be registered

    runs = workflow.get_runs()
    if runs.totalCount == 0:
        raise AirflowException("No runs found for the specified workflow.")

    # Get the latest run
    workflow_run_id = runs[0].id
    workflow_url = f"https://github.com/{repo_name}/actions/runs/{workflow_run_id}"
    print(workflow_url)  # Print the URL of the workflow

    # Loop to check the status of the workflow run until it completes
    while True:
        workflow_run = repo.get_workflow_run(workflow_run_id)
        status = workflow_run.status  # Current status of the workflow run
        conclusion = workflow_run.conclusion  # Final outcome if completed

        if status == 'completed':
            if conclusion == 'success':
                print("Workflow completed successfully.")
                return
            else:
                raise AirflowException("The GitHub workflow failed.")
        else:
            print(f"Current status: {status}. Waiting for completion...")
            time.sleep(60)  # Wait before checking again
