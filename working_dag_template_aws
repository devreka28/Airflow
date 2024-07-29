import pendulum
import logging

from airflow.decorators import dag, task
from airflow.secrets.metastore import MetastoreBackend
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from udacity.common import sql_statements
from udacity.common import final_project_sql_statements

@dag(
    start_date=pendulum.now()
)
def final_project():

    @task
    def drop_table_task3():
        redshift_hook=PostgresHook("redshift")
        redshift_hook.run('DROP TABLE IF EXISTS events_source3')

    create_table_task=PostgresOperator(
       task_id="create_table",
        postgres_conn_id="redshift",
        sql=sql_statements.CREATE_TRIPS_TABLE_SQL
    )

    create_table_task1=PostgresOperator(
       task_id="create_table1",
        postgres_conn_id="redshift",
        sql=final_project_sql_statements.staging_events_table_create2
        
    )
    
    @task
    def first_log():
        logging.info("Table Create 1 successful.")

    create_table_task2=PostgresOperator(
       task_id="create_table2",
        postgres_conn_id="redshift",
        sql=final_project_sql_statements.SqlQueries.staging_events_table_create
    )

    @task
    def second_log():
        logging.info("Table Create 2 successful.")
    
    @task
    def create_table_task3():
        redshift_hook=PostgresHook("redshift")
        redshift_hook.run(final_project_sql_statements.SqlQueries.staging_events_table_create3)

    drop_table_task3=drop_table_task3()
    load_event_data_task=load_event_data_task()
    first_log=first_log()
    second_log=second_log()
    create_table_task3=create_table_task3()

    create_table_task >> create_table_task1  
    create_table_task1 >> first_log
    first_log >> create_table_task2
    create_table_task2 >> second_log
    second_log >> drop_table_task3
    drop_table_task3 >> create_table_task3

final_project_dag = final_project()    
