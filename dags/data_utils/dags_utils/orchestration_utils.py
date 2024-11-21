from airflow import DAG
from airflow.models import DagBag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule


def is_dag_active_and_exists(dag_id, **kwargs):
    """
    Returns True if the DAG is active (not paused), False otherwise.
    """
    dag_bag = DagBag()

    dag = dag_bag.get_dag(dag_id)

    if not dag or dag.is_paused:  # Check if the DAG is missing or paused
        print(f"DAG {dag_id} not found or paused. Skipping...")
        return False
    # If the DAG exists and is active
    return True


def branch_on_dag_status(dag_id, **kwargs):
    """
    Determines which branch to take based on DAG status.
    """
    if is_dag_active_and_exists(dag_id):
        print(f"DAG {dag_id} is active. Proceeding to trigger...")
        return f"trigger_{dag_id}"
    else:
        print(f"DAG {dag_id} is paused or missing. Skipping...")
        return f"skip_{dag_id}"


def create_orchestration_dag(dag_id, description, schedule_interval, start_date, dags_to_orchestrate):
    """
    Function to create an orchestration DAG dynamically.

    :param dag_id: ID of the DAG
    :param description: Description of the DAG
    :param schedule_interval: Schedule interval of the DAG
    :param start_date: Start date of the DAG
    :param dags_to_orchestrate: List of DAGs to orchestrate
    :return: An Airflow DAG object
    """
    dag = DAG(
        dag_id=dag_id,
        description=description,
        schedule_interval=schedule_interval,
        start_date=start_date,
        catchup=False,
    )

    previous_task = None

    for dag_to_orchestrate_id in dags_to_orchestrate:
        # Branching task
        branch_task = BranchPythonOperator(
            task_id=f'branch_{dag_to_orchestrate_id}',
            python_callable=branch_on_dag_status,
            op_kwargs={'dag_id': dag_to_orchestrate_id},
            provide_context=True,
            dag=dag,
        )

        # Trigger task
        trigger_task = TriggerDagRunOperator(
            task_id=f'trigger_{dag_to_orchestrate_id}',
            trigger_dag_id=dag_to_orchestrate_id,
            wait_for_completion=True,
            reset_dag_run=True,
            poke_interval=60,
            dag=dag,
        )

        # Skip task
        skip_task = PythonOperator(
            task_id=f'skip_{dag_to_orchestrate_id}',
            python_callable=lambda: print(f"Skipping {dag_to_orchestrate_id}"),
            dag=dag,
        )

        # Dummy task
        dummy_task = EmptyOperator(
            task_id=f'dummy_{dag_to_orchestrate_id}',
            dag=dag,
            trigger_rule=TriggerRule.ALL_DONE
        )

        # Define dependencies
        branch_task >> [trigger_task, skip_task]
        trigger_task >> dummy_task
        skip_task >> dummy_task

        if previous_task:
            previous_task >> branch_task
        previous_task = dummy_task

    return dag
