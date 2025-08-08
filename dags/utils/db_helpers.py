import time
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowException

from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

def get_db_hook(conn_id: str, source_type: str):
    """
    A dispatcher function that returns a resilient hook for a given DB type.
    It calls 'get_hook_with_retry' with the appropriate Hook class.
    """
    if source_type == 'oracle': 
        return get_hook_with_retry(OracleHook, conn_id)
    if source_type == 'mssql': 
        return get_hook_with_retry(MsSqlHook, conn_id)
    if source_type in ['mysql', 'maria']: 
        return get_hook_with_retry(MySqlHook, conn_id)
    if source_type == 'postgres': 
        return get_hook_with_retry(PostgresHook, conn_id)
    # If the database type is not supported, raise an error.
    raise ValueError(f"Unsupported DB type: {source_type}")

def get_hook_with_retry(hook_class, conn_id):
    """
    A helper function to get a database hook with a retry mechanism.
    It attempts an initial connection and then retries 3 more times upon failure.

    :param hook_class: The Airflow Hook class to instantiate (e.g., OracleHook).
    :param conn_id: The Airflow connection ID for the database.
    :param retries: Total number of attempts (1 initial + (n-1) retries).
    :param delay: The number of seconds to wait between retries.
    :return: An instantiated and connected Hook object.
    :raises AirflowException: If the connection fails after all retry attempts.
    """
    # Get the logger from the current Airflow task instance context.
    log = get_current_context()['ti'].log

    # Get retry options from Airflow Variable
    try:
        retries = int(Variable.get("db_connect_retries", default_var=4))
        delay = int(Variable.get("db_connect_retry_delay", default_var=15))
    except (ValueError, TypeError):
        log.warning("Could not parse retry variables. Using default values (4 retries, 15s delay).")
        retries = 4
        delay = 15

    # Loop for the specified number of retries.
    for i in range(retries):
        try:
            log.info(f"Attempting to get hook for {conn_id} (Attempt {i + 1}/{retries})")
            hook = hook_class(conn_id)
            # Test the connection by trying to create a SQLAlchemy engine.
            # This is more reliable than just instantiating the hook
            hook.get_sqlalchemy_engine() 
            log.info(f"Successfully connected to {conn_id}.")
            return hook
        except Exception as e:
            # If an exception occurs, log it as a warning.
            log.warning(f"Failed to connect to {conn_id}: {e}")

            # If this wasn't the last attempt, wait for the specified delay.
            if i < retries - 1:
                log.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            # If it was the last attempt, log an error and re-raise the exception to fail the task.
            else:
                log.error(f"Could not connect to {conn_id} after {retries} attempts.")
                raise