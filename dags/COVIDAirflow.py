"""
    Download, extract, and analyze all the COVID data:
    1) Download all the datasets
    2) Extract the data from the datasets

    Require the BeautifulSoup and pandas library: pip install beautifulsoup4 pandas
"""
# region Libraries import
# Python internal libraries
import os
from datetime import datetime as dt, timedelta as td

# Airflow libraries
from airflow import DAG
from airflow.operators.dummy import DummyOperator

# endregion

# region Airflow DAG definition
from taskgroups.GoogleDatasets import GoogleDatasetsTaskGroup

dag_name = 'COVIDWorkflow'
start_date = dt(2021, 1, 1)
default_args = {'owner': 'airflow', 'retries': 3, 'retry_delay': td(seconds=30)}

dag = DAG(
    dag_name,
    default_args=default_args,
    description='Download, extract, and analyze all the COVID data',
    schedule_interval=td(days=1),
    start_date=start_date,
    catchup=False
)

os.chdir('/home/airflow/covid')  # download the datasets into subfolders of the "covid" folder


# endregion

# region Airflow operators instantiation
dummy_start_op = DummyOperator(task_id='start', dag=dag)
dummy_end_op = DummyOperator(task_id='end', dag=dag)

google_data = GoogleDatasetsTaskGroup(dag)

# endregion

# region Airflow pipeline definition

dummy_start_op >> google_data >> dummy_end_op

# endregion
