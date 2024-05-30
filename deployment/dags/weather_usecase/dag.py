import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

from weather_usecase.etl.extract_functions import (
    extract_cities_coordinates, extract_weather, extract_uv_index,
    extract_air_quality_index, extract_historical_temperature, extract_historical_precipitation
)
from weather_usecase.etl.transform_functions import (
    transform_city_coordinates, transform_weather, transform_uv_index,
    transform_air_quality_index, transform_historical_temperature, transform_historical_precipitation
)
from weather_usecase.etl.aggregate_functions import (
    aggregate_extracted_data, aggregate_transformed_data
)
from weather_usecase.etl.load_functions import (
    write_city_coordinates_to_db, write_weather_to_db, write_uv_index_to_db,
    write_air_quality_index_to_db, write_historical_temperature_to_db, write_historical_precipitation_to_db
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 5,
    'retry_delay': timedelta(minutes=15),
}

def pull_xcom_value(task_instance, task_id, key):
    return task_instance.xcom_pull(task_ids=task_id, key=key)

def run_transformations(**context):
    task_instance = context['task_instance']
    aggregated_data = task_instance.xcom_pull(task_ids='aggregate_extracted_data', key='return_value')

    cities_df, coordinates_df, weather_df, uv_index_df, air_quality_index_df, historical_temperature_df, historical_precipitation_df = aggregated_data

    city_coordinates_df = transform_city_coordinates(cities_df, coordinates_df)
    weather_df = transform_weather(weather_df)
    uv_index_df = transform_uv_index(uv_index_df)
    air_quality_index_df = transform_air_quality_index(air_quality_index_df)
    historical_temperature_df = transform_historical_temperature(historical_temperature_df)
    historical_precipitation_df = transform_historical_precipitation(historical_precipitation_df)

    return city_coordinates_df, weather_df, uv_index_df, air_quality_index_df, historical_temperature_df, historical_precipitation_df

def run_write_operations(**context):
    task_instance = context['task_instance']
    transformed_data = task_instance.xcom_pull(task_ids='run_transformations', key='return_value')

    city_coordinates_df, weather_df, uv_index_df, air_quality_index_df, historical_temperature_df, historical_precipitation_df = transformed_data

    write_city_coordinates_to_db(city_coordinates_df)
    write_weather_to_db(weather_df)
    write_uv_index_to_db(uv_index_df)
    write_air_quality_index_to_db(air_quality_index_df)
    write_historical_temperature_to_db(historical_temperature_df)
    write_historical_precipitation_to_db(historical_precipitation_df)

with DAG('weather_data_pipeline', default_args=default_args, schedule_interval=None) as dag:
    extract_cities_coordinates_task = PythonOperator(
        task_id='extract_cities_coordinates',
        python_callable=extract_cities_coordinates,
    )

    extract_weather_task = PythonOperator(
        task_id='extract_weather',
        python_callable=extract_weather,
    )

    extract_uv_index_task = PythonOperator(
        task_id='extract_uv_index',
        python_callable=extract_uv_index,
    )

    extract_air_quality_index_task = PythonOperator(
        task_id='extract_air_quality_index',
        python_callable=extract_air_quality_index,
    )

    extract_historical_temperature_task = PythonOperator(
        task_id='extract_historical_temperature',
        python_callable=extract_historical_temperature,
    )

    extract_historical_precipitation_task = PythonOperator(
        task_id='extract_historical_precipitation',
        python_callable=extract_historical_precipitation,
    )

    aggregate_extracted_data_task = PythonOperator(
        task_id='aggregate_extracted_data',
        python_callable=aggregate_extracted_data,
    )

    run_transformations_task = PythonOperator(
        task_id='run_transformations',
        python_callable=run_transformations,
        provide_context=True,
        execution_timeout=timedelta(minutes=30),
    )

    run_write_operations_task = PythonOperator(
        task_id='run_write_operations',
        python_callable=run_write_operations,
        provide_context=True,
    )

    [extract_cities_coordinates_task, extract_weather_task, extract_uv_index_task, 
     extract_air_quality_index_task, extract_historical_temperature_task, 
     extract_historical_precipitation_task] >> aggregate_extracted_data_task

    aggregate_extracted_data_task >> run_transformations_task
    run_transformations_task >> run_write_operations_task
