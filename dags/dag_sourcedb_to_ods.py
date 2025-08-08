import logging
import pandas as pd
import re

from datetime import datetime
from airflow.decorators import dag, task_group, task
from airflow.models import Variable
from airflow.operators.python import get_current_context, ShortCircuitOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException

from airflow.providers.oracle.hooks.oracle import OracleHook
from utils.db_helpers import get_hook_with_retry, get_db_hook

## ---------------------------------------------------------------------------------
## Global Configurations
## ---------------------------------------------------------------------------------
# Defines the number of rows to write in a single batch.
# This helps manage memory usage when dealing with large tables.
CHUNK_SIZE = int(Variable.get("etl_chunk_size", default_var=10000))

def sanitize_table_name(table_name: str) -> str:
    """
    Validates and sanitizes table names to prevent SQL injection.
    """
    if not re.match(r'^[a-zA-Z0-9_.]+$', table_name):
        raise AirflowException(f"Invalid table name format: {table_name}")
    return table_name

def validate_column_name(column_name: str) -> str:
    """
    Validates and sanitizes column names to prevent SQL injection.
    """
    if not re.match(r'^[a-zA-Z0-9_]+$', column_name):
        raise AirflowException(f"Invalid column name format: {column_name}")
    return column_name

## ---------------------------------------------------------------------------------
## DAG Definition
## ---------------------------------------------------------------------------------
@dag(
    dag_id='DAG_SOURCEDB_TO_EDW',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    doc_md="[Worker] Loads data from source DBs to EDW based on the `etl_config` variable.",
    tags=['worker', 'etl']
)
def worker_etl_to_edw():
    """
    This DAG performs the main ETL process. It dynamically creates tasks
    based on the 'etl_config' Airflow Variable.
    """
    # Empty operators for graph structure
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end', trigger_rule=TriggerRule.ALL_DONE)

    # Get DAG execution timestamp once for all tasks (data consistency)
    dag_execution_time = datetime.now()

    # Load the ETL configuration from an Airflow Variable.
    # This variable should be a JSON containing a list of tables to process.
    try:
        etl_info = Variable.get("etl_config", deserialize_json=True)
        tables_to_process = etl_info.get("tables", [])
    except Exception as e:
        logging.error(f"Failed to load Airflow Variable 'etl_config': {e}")
        tables_to_process = []

    def check_if_targets_exist(**kwargs):
        """
        This function is called by the ShortCircuitOperator. It returns True if there
        are tables to process, otherwise False, skipping all downstream tasks.
        """
        if tables_to_process:
            return True
        else:
            kwargs['ti'].log.warning("ETL target list is empty in 'etl_config' variable. Skipping downstream tasks.")
            return False

    # This operator acts as a gatekeeper for the DAG.
    check_etl_targets = ShortCircuitOperator(
        task_id='check_if_etl_targets_exist',
        python_callable=check_if_targets_exist,
    )

    # A list to hold all dynamically created task groups.
    etl_task_groups = []

    # Loop through each table defined in the configuration to create tasks dynamically.
    for table_info in tables_to_process:
        
        def create_etl_task_group(info: dict, execution_time: datetime):
            """Factory function to create ETL task groups"""
            # Extract and validate table-specific info from the dictionary.
            db_type = info['db_type']
            source_conn = info['source_conn_id']
            source_table = sanitize_table_name(info['source_table'])
            target_table = sanitize_table_name(info['target_table'])
            
            @task_group(group_id=f'etl__{source_table.replace(".", "_")}')
            def etl_process_group():
                
                @task
                def truncate_table_task():
                    """Task to truncate the target table before loading new data."""
                    log = get_current_context()['ti'].log

                    # Get target DB(ODS) information from Airflow Variable
                    etl_target_conn_id = Variable.get("etl_target_conn_id")

                    log.info(f"Attempting to TRUNCATE table: {target_table.upper()}")
                    
                    try:
                        # Get the database hook using the retry mechanism.
                        hook = get_hook_with_retry(OracleHook, etl_target_conn_id)

                        # Execute the TRUNCATE statement.
                        hook.run(f'TRUNCATE TABLE {target_table.upper()}', autocommit=True)

                        log.info(f"Table {target_table.upper()} truncated successfully.")
                        
                    except Exception as e:
                        log.error(f"Failed to truncate table {target_table.upper()}: {e}")
                        raise

                @task
                def transfer_data_task():
                    """Task to transfer data from a source table to a target table."""
                    log = get_current_context()['ti'].log
                    etl_target_conn_id = Variable.get("etl_target_conn_id")

                    # Initialize variables to avoid UnboundLocalError
                    pandas_success = False
                    total_rows = 0
                    
                    # Use the DAG execution time passed from parent scope
                    current_timestamp = execution_time
                    log.info(f"Using consistent ODS_LOAD_DT timestamp: {current_timestamp}")
                    
                    try:
                        source_hook = get_db_hook(source_conn, db_type)
                        target_hook = get_hook_with_retry(OracleHook, etl_target_conn_id)

                        # Step 1: Get target table column information
                        schema_name, table_name_only = target_table.upper().split('.')
                        log.info(f"Fetching column list for Owner: {schema_name}, Table: {table_name_only}")
                        
                        column_query = """
                            SELECT COLUMN_NAME 
                            FROM ALL_TAB_COLUMNS 
                            WHERE OWNER = :owner_name AND TABLE_NAME = :table_name
                            ORDER BY COLUMN_ID
                        """
                        target_columns_records = target_hook.get_records(
                            column_query, 
                            parameters={'owner_name': schema_name, 'table_name': table_name_only}
                        )
                        
                        if not target_columns_records:
                            raise AirflowException(f"No columns found for table {target_table.upper()}")
                        
                        target_columns_list = [validate_column_name(rec[0]) for rec in target_columns_records]
                        log.info(f"Target columns found: {target_columns_list}")

                        # Check if ODS_LOAD_DT column exists in target table
                        has_ods_load_dt = 'ODS_LOAD_DT' in target_columns_list
                        log.info(f"ODS_LOAD_DT column exists in target table: {has_ods_load_dt}")

                        # Step 2: Build SELECT query (exclude ODS_LOAD_DT from source query)
                        source_columns_list = [col for col in target_columns_list if col != 'ODS_LOAD_DT']
                        
                        if db_type.lower() == 'oracle':
                            safe_columns = [f'"{col}"' for col in source_columns_list]
                        elif db_type.lower() in ['mysql', 'mariadb']:
                            safe_columns = [f'`{col}`' for col in source_columns_list]
                        elif db_type.lower() in ['postgresql']:
                            safe_columns = [f'"{col.lower()}"' for col in source_columns_list]
                        else:
                            safe_columns = source_columns_list
                        
                        select_columns_str = ", ".join(safe_columns)
                        
                        # Build safe table name based on database type
                        if db_type.lower() in ['mysql', 'mariadb']:
                            safe_table_name = f"`{source_table}`"
                        elif db_type.lower() in ['postgresql']:
                            safe_table_name = f'"{source_table.lower()}"'
                        else:
                            safe_table_name = source_table
                        
                        query = f"SELECT {select_columns_str} FROM {safe_table_name}"
                        log.info(f"Executing query: {query}")

                        # Step 3: Data transfer with fallback mechanism
                        try:
                            log.info("Attempting Pandas/SQLAlchemy method...")
                            source_engine = source_hook.get_sqlalchemy_engine()
                            target_engine = target_hook.get_sqlalchemy_engine()
                            
                            # Pandas data transfer
                            for chunk_df in pd.read_sql_query(query, source_engine, chunksize=CHUNK_SIZE):
                                chunk_df.columns = [c.upper() for c in chunk_df.columns]
                                
                                # Add ODS_LOAD_DT column if it exists in target table
                                if has_ods_load_dt:
                                    chunk_df['ODS_LOAD_DT'] = current_timestamp
                                
                                chunk_df.to_sql(
                                    target_table.upper(), 
                                    target_engine, 
                                    if_exists='append', 
                                    index=False,
                                    method='multi'
                                )
                                total_rows += len(chunk_df)
                                log.info(f"Transferred {len(chunk_df)} rows. (Total: {total_rows})")
                            
                            pandas_success = True
                            log.info("Pandas method completed successfully!")
                            
                        except Exception as pandas_error:
                            log.warning(f"Pandas method failed: {pandas_error}")
                            log.info("Falling back to Hook-only method...")
                            pandas_success = False

                        # Fallback to Hook method if Pandas failed
                        if not pandas_success:
                            log.info("Using Hook-only method")
                            
                            # Get data from source using Hook
                            source_data = source_hook.get_records(query)
                            total_rows = len(source_data)
                            log.info(f"Fetched {total_rows} rows from source using Hook method")
                            
                            if total_rows > 0:
                                # Prepare INSERT statement
                                if has_ods_load_dt:
                                    # Include ODS_LOAD_DT in insert
                                    insert_columns = ", ".join(target_columns_list)
                                    placeholders = ", ".join([f":col{i+1}" for i in range(len(target_columns_list))])
                                else:
                                    # Exclude ODS_LOAD_DT from insert
                                    insert_columns = ", ".join(source_columns_list)
                                    placeholders = ", ".join([f":col{i+1}" for i in range(len(source_columns_list))])
                                
                                insert_query = f"INSERT INTO {target_table.upper()} ({insert_columns}) VALUES ({placeholders})"
                                log.info(f"Insert query: {insert_query}")
                                
                                # Process data in chunks
                                inserted_rows = 0
                                conn = target_hook.get_conn()
                                cursor = conn.cursor()
                                
                                try:
                                    for i in range(0, total_rows, CHUNK_SIZE):
                                        chunk_data = source_data[i:i + CHUNK_SIZE]
                                        
                                        # Convert tuples to list format for Oracle and add ODS_LOAD_DT if needed
                                        processed_chunk = []
                                        for row in chunk_data:
                                            row_list = list(row)
                                            if has_ods_load_dt:
                                                row_list.append(current_timestamp)  # Add ODS_LOAD_DT
                                            processed_chunk.append(row_list)
                                        
                                        # Execute batch insert using executemany
                                        cursor.executemany(insert_query, processed_chunk)
                                        inserted_rows += len(processed_chunk)
                                        
                                        progress = (inserted_rows / total_rows) * 100
                                        log.info(f"Inserted {len(processed_chunk)} rows using Hook method. Progress: {inserted_rows}/{total_rows} ({progress:.1f}%)")
                                    
                                    # Commit all changes
                                    conn.commit()
                                    log.info("All data committed successfully.")
                                    
                                finally:
                                    cursor.close()
                                    conn.close()
                            else:
                                log.info("No data to transfer.")
                        
                        log.info(f"Transfer complete. Total rows transferred: {total_rows}")
                        
                    except Exception as e:
                        log.error(f"Error during data transfer from {source_table} to {target_table}: {e}")
                        # Rollback on error if using Hook method
                        if not pandas_success:
                            try:
                                # Get connection and rollback
                                conn = target_hook.get_conn()
                                conn.rollback()
                                conn.close()
                                log.info("Transaction rolled back successfully.")
                            except Exception as rollback_error:
                                log.error(f"Failed to rollback transaction: {rollback_error}")
                        raise
                
                truncate_table_task() >> transfer_data_task()
            
            return etl_process_group
        
        # Create and add the task group to our list (pass execution time)
        etl_task_group = create_etl_task_group(table_info, dag_execution_time)()
        etl_task_groups.append(etl_task_group)
    
    # Set the main dependency for the DAG.
    if etl_task_groups:
        start >> check_etl_targets >> etl_task_groups >> end

# Instantiate the DAG.
worker_etl_to_edw()