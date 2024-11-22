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


def trigger_workflow(workflow_file, token):
    """
    Trigger a GitHub Actions workflow and monitor its execution.

    :param workflow_file: Name of the GitHub Actions workflow YAML file.
    :param token: GitHub Personal Access Token for authentication.
    """
    g = Github(token)  # Initialize the GitHub instance with the provided token
    repo_name = Variable.get("dbt_repository_name")  # Get repository name from Airflow variables
    repo = g.get_repo(repo_name)  # Access the repository
    workflow = repo.get_workflow(workflow_file)  # Select the workflow based on the provided YAML file name

    # Trigger the workflow
    workflow.create_dispatch(ref='main')  # Assuming the 'main' branch is the target
    time.sleep(5)  # Wait briefly for the workflow run to register

    # Fetch the latest workflow runs
    runs = workflow.get_runs()
    if runs.totalCount == 0:
        raise AirflowException("No runs found for the specified workflow.")

    # Get the latest run
    workflow_run_id = runs[0].id
    workflow_url = f"https://github.com/{repo_name}/actions/runs/{workflow_run_id}"
    print(f"Workflow URL: {workflow_url}")

    # Monitor the workflow run status
    while True:
        workflow_run = repo.get_workflow_run(workflow_run_id)
        status = workflow_run.status  # 'queued', 'in_progress', 'completed'
        conclusion = workflow_run.conclusion  # 'success', 'failure', etc.

        if status == 'completed':
            if conclusion == 'success':
                print("Workflow completed successfully.")
                return  # Exit if the workflow completes successfully
            else:
                raise AirflowException(f"The GitHub workflow failed with conclusion: {conclusion}.")
        else:
            print(f"Current status: {status}. Waiting for completion...")
            time.sleep(60)  # Check again in 1 minute
