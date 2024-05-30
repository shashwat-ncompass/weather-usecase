import pandas as pd
from weather_usecase.utils.response_handler import handle_response, handle_error

def transform_city_coordinates(cities_df, coordinates_df):
    try:
        city_coordinates_df = pd.merge(cities_df, coordinates_df, on='city_id')
        handle_response(city_coordinates_df, 'transform_city_coordinates')
        return city_coordinates_df
    except Exception as e:
        handle_error(e, 'transform_city_coordinates')
        raise

def transform_weather(weather_df):
    try:
        weather_df = weather_df.round(2)
        weather_df['date'] = pd.to_datetime(weather_df['date'])
        weather_df['particular_date'] = weather_df['date'].dt.date
        weather_df['time'] = weather_df['date'].dt.time
        weather_df.drop('date', axis=1, inplace=True)
        handle_response(weather_df, 'transform_weather')
        return weather_df
    except Exception as e:
        handle_error(e, 'transform_weather')
        raise

def transform_uv_index(uv_index_df):
    try:
        uv_index_df = uv_index_df.round(2)
        uv_index_df['date'] = pd.to_datetime(uv_index_df['date'])
        uv_index_df['particular_date'] = uv_index_df['date'].dt.date
        uv_index_df['time'] = uv_index_df['date'].dt.time
        uv_index_df.drop('date', axis=1, inplace=True)
        handle_response(uv_index_df, 'transform_uv_index')
        return uv_index_df
    except Exception as e:
        handle_error(e, 'transform_uv_index')
        raise

def transform_air_quality_index(air_quality_index_df):
    try:
        air_quality_index_df = air_quality_index_df.round(2)
        air_quality_index_df['date'] = pd.to_datetime(air_quality_index_df['date'])
        air_quality_index_df['particular_date'] = air_quality_index_df['date'].dt.date
        air_quality_index_df['time'] = air_quality_index_df['date'].dt.time
        air_quality_index_df.drop('date', axis=1, inplace=True)
        handle_response(air_quality_index_df, 'transform_air_quality_index')
        return air_quality_index_df
    except Exception as e:
        handle_error(e, 'transform_air_quality_index')
        raise

def transform_historical_temperature(historical_temperature_df):
    try:
        historical_temperature_df['date'] = pd.to_datetime(historical_temperature_df['date'])
        historical_temperature_df['particular_date'] = historical_temperature_df['date'].dt.date
        historical_temperature_df['time'] = historical_temperature_df['date'].dt.time
        historical_temperature_df.drop('date', axis=1, inplace=True)
        handle_response(historical_temperature_df, 'transform_historical_temperature')
        return historical_temperature_df
    except Exception as e:
        handle_error(e, 'transform_historical_temperature')
        raise

def transform_historical_precipitation(historical_precipitation_df):
    try:
        historical_precipitation_df['date'] = pd.to_datetime(historical_precipitation_df['date'])
        historical_precipitation_df['particular_date'] = historical_precipitation_df['date'].dt.date
        historical_precipitation_df['time'] = historical_precipitation_df['date'].dt.time
        historical_precipitation_df.drop('date', axis=1, inplace=True)
        handle_response(historical_precipitation_df, 'transform_historical_precipitation')
        return historical_precipitation_df
    except Exception as e:
        handle_error(e, 'transform_historical_precipitation')
        raise
