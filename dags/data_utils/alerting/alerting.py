from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.models import Variable
import urllib
import json

def task_failed(context):
    # Get run information
    values = {}
    values["dag_id"] = context["dag"].dag_id
    values["schedule_date"] = context["dag_run"].execution_date.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    values["running_date"] = context["dag_run"].start_date.strftime("%Y-%m-%d %H:%M:%S")
    values["airflow_url"] = Variable.get("airflow_url")
    values["url_param_date"] = urllib.parse.quote(values["schedule_date"])

    for ti in context["dag_run"].get_task_instances():
        if ti.state == "failed":
            values["task_id"] = ti.task_id

    # Format message
    message = """:red_circle: DAG run fail:
    - *DAG*: {dag_id}
    - *Task*: {task_id}
    - *Schedule date*: {schedule_date}
    - *Running  date*: {running_date}

    {airflow_url}/log?dag_id={dag_id}&task_id={task_id}&execution_date={url_param_date}
    """.format(
        **values
    )

    # Create Slack message and send it
    return send_webhook_alert(message=message).execute(
        context=context
    )


def send_webhook_alert(message, context):
    """
    This function sends a POST request to a webhook when a task fails.
    It uses the context provided by Airflow to include information about the DAG and task.
    """

    return SimpleHttpOperator(
        task_id='send_failure_webhook',
        http_conn_id='my_webhook_connection',  # This should be the connection ID you set up in Airflow
        endpoint='',
        method='POST',
        data=json.dumps(message),
        headers={"Content-Type": "application/json"},
        dag=context['dag'],
    )