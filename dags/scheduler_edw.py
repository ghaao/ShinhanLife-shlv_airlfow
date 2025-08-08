from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

### Alert Mail Send Setting ###
# 1. Load the list of email recipients from an Airflow Variable.
# 2. Use a default email if the Variable is not set, then split the string by comma to create a list.
# try:
#     recipient_emails_str = Variable.get("alert_email_recipients", default_var='airflow-alerts@example.com')
#     recipient_emails_list = [email.strip() for email in recipient_emails_str.split(',')]
# except Exception:
#     recipient_emails_list = ['airflow-alerts@example.com']

## ---------------------------------------------------------------------------------
## Default Arguments & Configurations
## ---------------------------------------------------------------------------------
# Define default arguments that will be applied to all tasks in the DAG.
default_args = {
    'owner': 'AI Data Chapter',
    'retries': 3,                           # Number of retries for a task if it fails.
    'retry_delay': timedelta(seconds=15),   # Time to wait between retries.
    # 'email': ['your-alert-email@example.com'], 
    # 'email_on_failure': True,
    # 'email_on_retry': False,
}

# Load the DAG's execution schedule from an Airflow Variable.
# This allows changing the schedule without modifying the code.
# It falls back to a default schedule if the variable is not found.
try:
    schedule_variable = Variable.get("etl_schedule", default_var='0 5 * * *')
    schedule_interval = None if str(schedule_variable).lower() == 'none' else schedule_variable
except Exception:
    schedule_interval = '0 5 * * *'

## ---------------------------------------------------------------------------------
## DAG Definition
## ---------------------------------------------------------------------------------
@dag(
    dag_id='SCHEDULER_EDW',             # Unique identifier for the DAG.
    default_args=default_args,          # Apply the default arguments defined above.
    start_date=datetime(2025, 1, 1),    # The date from which the DAG will start running.
    schedule=schedule_interval,         # Set the execution schedule.
    catchup=False,                      # If True, Airflow would run past, unscheduled DAG runs. False is usually preferred.
    doc_md="""
    ### Daily Data Pipeline Controller
    This DAG orchestrates the ETL and Mart creation jobs sequentially.
        1. Triggers the WORKER_ETL_TO_EDW DAG and waits for its completion.
        2. Triggers the WORKER_MART_CREATION_TO_EDW DAG and waits for its completion.
    """,                                # A markdown description for the Airflow UI.
    tags=['controller']                 # Tags to help organize and filter DAGs in the UI.
)
def controller_etl_mart_pipeline():
    """
    This is the main DAG function that acts as a controller.
    It does not perform any data processing itself but triggers worker DAGs in order.
    """
    # Empty DAG for graph
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end',  trigger_rule=TriggerRule.ALL_DONE)

    # STEP 1: Trigger the first worker DAG that handles the ETL process.
    trigger_sourcedb_to_edw = TriggerDagRunOperator(
        trigger_dag_id='DAG_SOURCEDB_TO_EDW',
        task_id='trigger_sourcedb_to_edw',
        wait_for_completion=True,   # This is crucial: the controller will wait for the triggered DAG to complete.
        poke_interval=30,           # How often (in seconds) to check the status of the triggered DAG.
        deferrable=True,            # This mode releases the worker slot while waiting, saving resources. Requires the Airflow 'triggerer' component to be running.
        reset_dag_run=True
    )

    # STEP 2: Trigger the second worker DAG that creates the data mart.
    trigger_ods_to_mart = TriggerDagRunOperator(
        trigger_dag_id='DAG_ODS_TO_MART',
        task_id='trigger_ods_to_mart',
        wait_for_completion=True,
        poke_interval=30,
        deferrable=True,
        reset_dag_run=True
    )

    # Define the execution order: the ETL worker must complete before the Mart worker begins.
    start >> trigger_sourcedb_to_edw >> trigger_ods_to_mart >> end

# Instantiate the DAG object to be picked up by Airflow.
controller_etl_mart_pipeline()