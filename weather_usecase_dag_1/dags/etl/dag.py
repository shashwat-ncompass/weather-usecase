import sys
from os.path import dirname, abspath
sys.path.append(dirname(abspath(__file__)))

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

from db_operations.creating_tables import create_tables_sqlite
from db_operations.storing_cities import insert_city_data
from extract_data.weather_data import get_weather_data
from extract_data.aqi_data import get_aqi_data
from extract_data.historical_data import get_historical_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 29),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'schedule_interval': '@once'
}

with DAG('etl_task_1', default_args=default_args, description='Dag to perform data extraction from API and store into sqlite tables') as dag:
    sqlite_table_creation = PythonOperator(
        task_id='create_tables',
        python_callable=create_tables_sqlite
    )

    insert_cities = PythonOperator(
        task_id='insert_cities_data',
        python_callable=insert_city_data
    )

    insert_weather_data = PythonOperator(
        task_id='insert_weather_data',
        python_callable=get_weather_data
    )

    insert_aqi_data = PythonOperator(
        task_id='insert_air_quality_data',
        python_callable=get_aqi_data
    )

    insert_historical_data = PythonOperator(
        task_id='insert_historical_weather_data',
        python_callable=get_historical_data
    )


sqlite_table_creation >> insert_cities >> insert_weather_data >> insert_aqi_data >> insert_historical_data