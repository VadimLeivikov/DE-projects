#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from airflow import DAG  
from airflow.providers.postgres.operators.postgres import PostgresOperator  
from airflow.operators.python import PythonOperator  
from airflow.hooks.postgres_hook import PostgresHook  
from airflow.utils.dates import days_ago  

# Dictionary of correspondence between SQL scripts and tables
script_to_tables = {  
    'ETL_DQ_nds.sql': ['nds.orders', 'nds.payments'],  
    'ETL_DQ_dds.sql': [  
        'fact.sales_items',  
        'dim.products',  
        'dim.payment_types',  
        'dim.functional_structure',  
        'dim.customers',  
    ],  
}  

# Function for logging metadata (without status)
def log_metadata_initial(**kwargs):  
    sql_script = kwargs['sql_script']  
    task_id = kwargs['task_instance'].task_id  
    dag_id = kwargs['dag'].dag_id  
    execution_date = kwargs['ts']  

    hook = PostgresHook(postgres_conn_id='postgres_netology')  
    tables = script_to_tables.get(sql_script, [])  

    for table in tables:  
        # Getting the previous row count from metadata
        last_count_query = """  
            SELECT last_row_count  
            FROM metadata.etl_status  
            WHERE target_table = %s  
            ORDER BY execution_date DESC LIMIT 1;  
        """  
        last_count = hook.get_first(last_count_query, parameters=(table,))  
        last_count = last_count[0] if last_count else 0  # Если данных нет, то 0  

        current_count = hook.get_first(f"SELECT COUNT(*) FROM {table}")[0]  

        # Determining the difference in rows
        rows_processed = current_count - last_count  

        sql = """  
            INSERT INTO metadata.etl_status (  
                dag_id, task_id, execution_date, rows_processed, target_table, last_row_count  
            ) VALUES (%s, %s, %s, %s, %s, %s);  
        """  
        hook.run(  
            sql,  
            parameters=(  
                dag_id, task_id, execution_date, rows_processed, table, current_count  
            )  
        )  

# Function for updating statuses after the DAG completion
def update_statuses():  
    hook_src = PostgresHook(postgres_conn_id='postgres_default')  
    hook_target = PostgresHook(postgres_conn_id='postgres_netology')  

    # Getting task statuses from the task_instance table
    status_query = """  
        SELECT dag_id, task_id, state  
        FROM public.task_instance  
        WHERE dag_id = 'etl_supermarketsales_DQ_metadata';  
    """  
    statuses = hook_src.get_records(status_query)  

    # Updating statuses in the metadata.etl_status table  
    update_query = """  
        UPDATE metadata.etl_status  
        SET status = %s  
        WHERE dag_id = %s AND task_id = %s;  
    """  
    for dag_id, task_id, state in statuses:  
        hook_target.run(update_query, parameters=(state, dag_id, task_id))  

# Default parameters for DAG
default_args = {  
    'owner': 'quantum',  
    'start_date': days_ago(1),  
    'retries': 1,  
}  

# Creating DAG
with DAG(  
    'etl_supermarketsales_DQ_metadata',  
    default_args=default_args,  
    schedule_interval='10 7 * * *',  
    catchup=False,  
) as dag:  

    # Data Quality check in "stage" layer
    DQ_stage = PostgresOperator(  
        task_id='DQ_stage',  
        postgres_conn_id='postgres_netology',  
        sql='sql/DQ_stage.sql',  
    )  

    # Data transfer to "nds" layer
    load_to_nds = PostgresOperator(  
        task_id='load_to_nds',  
        postgres_conn_id='postgres_netology',  
        sql='sql/ETL_DQ_nds.sql',  
    )  

    # Logging metadata for "load_to_nds" step 
    log_load_to_nds = PythonOperator(  
        task_id='log_load_to_nds',  
        python_callable=log_metadata_initial,  
        op_kwargs={'sql_script': 'ETL_DQ_nds.sql'},  
        provide_context=True,  
    )  

    # Data transfer to "dds" layer 
    load_to_dds = PostgresOperator(  
        task_id='load_to_dds',  
        postgres_conn_id='postgres_netology',  
        sql='sql/ETL_DQ_dds.sql',  
    )  

    # Logging metadata for "load_to_dds" step 
    log_load_to_dds = PythonOperator(  
        task_id='log_load_to_dds',  
        python_callable=log_metadata_initial,  
        op_kwargs={'sql_script': 'ETL_DQ_dds.sql'},  
        provide_context=True,  
    )  

    # updating statuses in metadata.etl_status
    update_status = PythonOperator(  
        task_id='update_status',  
        python_callable=update_statuses,  
    )  

    # Defining the order of task execution 
    DQ_stage >> load_to_nds >> log_load_to_nds >> load_to_dds >> log_load_to_dds >> update_status  


