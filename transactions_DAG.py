#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.utils.dates import days_ago

# Define the base path
base_path = '\home\transactions'

# Define default_args
default_args = {
    'owner': 'admin',
    'start_date': datetime(2023, 12, 3),
    'retries': 1,
}

# Create a DAG
dag = DAG(
    'transactions',
    default_args=default_args,
    schedule_interval=None,  # Set your desired schedule interval
    catchup=False,
)

# Task to run the data collection script
APIFlask = BashOperator(
    task_id='API',
    bash_command=f'python {base_path}/app.py',
    dag=dag,
)

# Task to run the Hadoop MapReduce job
ETL = BashOperator(
    task_id='ETL',
    bash_command=f'python {base_path}/ETL.py',
    dag=dag,
)

# Task to start the HBase Thrift server
Fraude_Detection = BashOperator(
    task_id='Fraude_Detection',
    bash_command=f'python {base_path}/Fraude_Detection.py',
    dag=dag,
)

# Set task dependencies
APIFlask >> ETL >> Fraude_Detection

if __name__ == "__main__":
    dag.cli()