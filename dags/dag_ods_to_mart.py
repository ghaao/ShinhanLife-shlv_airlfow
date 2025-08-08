import os

from datetime import datetime
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.operators.python import get_current_context
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.baseoperator import chain
from airflow.exceptions import AirflowException

from utils.db_helpers import get_hook_with_retry

## ---------------------------------------------------------------------------------
## DAG Definition
## ---------------------------------------------------------------------------------
@dag(
    dag_id='DAG_ODS_TO_MART',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    doc_md="[Worker] Creates data marts by executing scripts from the `/sql` directory sequentially.",
    tags=['worker', 'mart']
)
def worker_mart_creation_to_edw():
    """
    This DAG executes a series of SQL scripts found in a specified directory
    to build data marts. The scripts are executed in a sequential, alphabetical order.
    """
    # Empty operators for graph structure
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end', trigger_rule=TriggerRule.ALL_DONE)

    # Load the SQL directory path from an Airflow Variable.
    sql_dir = Variable.get("mart_sql_directory", default_var="/opt/airflow/dags/sql/create_mart")

    # Validate that the specified SQL directory actually exists on the worker.
    if not os.path.exists(sql_dir):
        raise AirflowException(f"SQL directory not found: {sql_dir}")

    # Find all files ending with '.sql' and sort them alphabetically.
    sql_files = sorted([f for f in os.listdir(sql_dir) if f.endswith('.sql')])

    # If the directory is empty or contains no .sql files, fail the DAG explicitly.
    if not sql_files:
        raise AirflowException(f"No SQL files found to execute in: {sql_dir}")
    
    # A list to hold all dynamically created task instances.
    sql_tasks = []

    # Loop through each found SQL file to create a dedicated task for it.
    for sql_file in sql_files:
        # Create a safe and valid task_id from the filename.
        task_id = f'run__{os.path.splitext(sql_file)[0].replace(" ", "_").replace("-", "_")}'
        
        def create_sql_task(file_name):
            """Factory function to create SQL execution tasks"""
            @task(task_id=f'run__{os.path.splitext(file_name)[0].replace(" ", "_").replace("-", "_")}')
            def execute_sql_file_task():
                """
                This task executes a single SQL file against the target database.
                """
                log = get_current_context()['ti'].log
                
                try:
                    # Get the target database connection ID from an Airflow Variable.
                    mart_target_conn_id = Variable.get("mart_target_conn_id")

                    log.info(f"Executing SQL file: {file_name} on Connection ID: {mart_target_conn_id}")
                    
                    # Construct the file path
                    sql_file_path = os.path.join(sql_dir, file_name)
                    
                    # Check if the file exists
                    if not os.path.exists(sql_file_path):
                        raise AirflowException(f"SQL file not found: {sql_file_path}")
                    
                    # Read the SQL file content
                    with open(sql_file_path, 'r', encoding='utf-8') as file:
                        sql_content = file.read().strip()
                    
                    if not sql_content:
                        log.warning(f"SQL file {file_name} is empty. Skipping execution.")
                        return
                    
                    log.info(f"SQL content to execute: {sql_content}")
                    
                    # Get the database hook using the resilient retry helper function.
                    hook = get_hook_with_retry(OracleHook, mart_target_conn_id)

                    # Split SQL content into individual statements for execution
                    # Oracle typically uses semicolons (;) to separate statements
                    statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
                    
                    # If no statements found, execute the entire content as a single statement
                    if not statements:
                        statements = [sql_content]
                    
                    # Execute each statement
                    for i, statement in enumerate(statements):
                        if statement:
                            log.info(f"Executing statement {i+1}/{len(statements)}: {statement[:100]}...")
                            
                            # Execute the SQL statement
                            hook.run(sql=statement, autocommit=True)
                            
                            log.info(f"Statement {i+1} executed successfully.")

                    log.info(f"Successfully executed all statements in {file_name}.")
                    
                except Exception as e:
                    log.error(f"Error executing SQL file {file_name}: {str(e)}")
                    raise AirflowException(f"Failed to execute {file_name}: {str(e)}")
            
            return execute_sql_file_task
        
        # Create and add the task to our list
        sql_task = create_sql_task(sql_file)()
        sql_tasks.append(sql_task)

    # If tasks were created, set their dependencies.
    if sql_tasks:
        # The 'chain' function links all tasks in the list sequentially.
        start >> sql_tasks[0]
        chain(*sql_tasks)
        sql_tasks[-1] >> end

# Instantiate the DAG object.
worker_mart_creation_to_edw()