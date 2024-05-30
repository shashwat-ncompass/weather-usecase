import pandas as pd
from weather_usecase.utils.db_connection import get_weather_sqlite_connection, get_historical_sqlite_connection
from weather_usecase.utils.response_handler import handle_response, handle_error

def extract_cities_coordinates():
    try:
        conn = get_weather_sqlite_connection()
        cities_df = pd.read_sql_query("SELECT * FROM cities", conn)
        coordinates_df = pd.read_sql_query("SELECT * FROM coordinates", conn)
        conn.close()
        handle_response((cities_df, coordinates_df), 'extract_cities_coordinates')
        return cities_df, coordinates_df
    except Exception as e:
        handle_error(e, 'extract_cities_coordinates')
        raise e

def extract_weather():
    try:
        conn = get_weather_sqlite_connection()
        weather_df = pd.read_sql_query("SELECT * FROM weather", conn)
        conn.close()
        handle_response(weather_df, 'extract_weather')
        return weather_df
    except Exception as e:
        handle_error(e, 'extract_weather')
        raise e

def extract_uv_index():
    try:
        conn = get_weather_sqlite_connection()
        uv_index_df = pd.read_sql_query("SELECT * FROM uv_index", conn)
        conn.close()
        handle_response(uv_index_df, 'extract_uv_index')
        return uv_index_df
    except Exception as e:
        handle_error(e, 'extract_uv_index')
        raise e

def extract_air_quality_index():
    try:
        conn = get_weather_sqlite_connection()
        air_quality_index_df = pd.read_sql_query("SELECT * FROM air_quality_index", conn)
        conn.close()
        handle_response(air_quality_index_df, 'extract_air_quality_index')
        return air_quality_index_df
    except Exception as e:
        handle_error(e, 'extract_air_quality_index')
        raise e

def extract_historical_temperature():
    try:
        conn = get_historical_sqlite_connection()
        historical_temperature_df = pd.read_sql_query("SELECT * FROM hist_temperature", conn)
        conn.close()
        handle_response(historical_temperature_df, 'extract_historical_temperature')
        return historical_temperature_df
    except Exception as e:
        handle_error(e, 'extract_historical_temperature')
        raise e

def extract_historical_precipitation():
    try:
        conn = get_historical_sqlite_connection()
        historical_precipitation_df = pd.read_sql_query("SELECT * FROM hist_precipitation", conn)
        conn.close()
        handle_response(historical_precipitation_df, 'extract_historical_precipitation')
        return historical_precipitation_df
    except Exception as e:
        handle_error(e, 'extract_historical_precipitation')
        raise e