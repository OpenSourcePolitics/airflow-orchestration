from airflow.models import Variable
import urllib
import json
import requests
import logging


def task_failed(context):
    values = {}
    ti = context["task_instance"]
    dag_run = context["dag_run"]

    values["dag_id"] = context["dag"].dag_id
    values["task_id"] = ti.task_id
    values["schedule_date"] = dag_run.execution_date.strftime("%Y-%m-%d %H:%M:%S")
    values["running_date"] = dag_run.start_date.strftime("%Y-%m-%d %H:%M:%S")
    values["airflow_url"] = Variable.get("airflow_url")
    values["url_param_date"] = urllib.parse.quote(values["schedule_date"])
    values["environment"] = Variable.get("environment")

    logging.info(f"Task failed for task_id: {values['task_id']}")

    message = f"""DAG run fail :
    
    - Environment: {values['environment']}
    - DAG: {values['dag_id']}
    - Task: {values['task_id']}
    - Schedule date: {values['schedule_date']}
    - Running  date: {values['running_date']}

    {values['airflow_url']}/log?dag_id={values['dag_id']}&task_id={values['task_id']}&execution_date={values['url_param_date']}
    """
    send_webhook_alert(message)


def send_webhook_alert(message):
    url = Variable.get("alerting_url")
    headers = {"Content-Type": "application/json"}
    data = json.dumps({"text": message})

    try:
        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to send webhook: {e}")
