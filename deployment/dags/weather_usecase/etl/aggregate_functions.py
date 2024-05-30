import pandas as pd

def aggregate_extracted_data(**kwargs):
    ti = kwargs['ti']
    
    cities_data = ti.xcom_pull(task_ids='extract_cities_coordinates', key='return_value')
    weather_df = ti.xcom_pull(task_ids='extract_weather', key='return_value')
    uv_index_df = ti.xcom_pull(task_ids='extract_uv_index', key='return_value')
    air_quality_index_df = ti.xcom_pull(task_ids='extract_air_quality_index', key='return_value')
    historical_temperature_df = ti.xcom_pull(task_ids='extract_historical_temperature', key='return_value')
    historical_precipitation_df = ti.xcom_pull(task_ids='extract_historical_precipitation', key='return_value')
    
    cities_df, coordinates_df = cities_data
    extracted_data = (
        cities_df,
        coordinates_df,
        weather_df,
        uv_index_df,
        air_quality_index_df,
        historical_temperature_df,
        historical_precipitation_df
    )
    
    ti.xcom_push(key='return_value', value=extracted_data)
    return extracted_data



def aggregate_transformed_data(city_coordinates_df, weather_df, uv_index_df, air_quality_index_df, historical_temperature_df, historical_precipitation_df):
    transformed_data = (
            city_coordinates_df, weather_df, uv_index_df, 
            air_quality_index_df, historical_temperature_df, historical_precipitation_df
    )
    return transformed_data