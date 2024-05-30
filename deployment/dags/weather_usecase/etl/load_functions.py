import pandas as pd
from sqlalchemy import inspect, Column, Integer, String, Float, Date, Time, ForeignKey

from weather_usecase.utils.db_connection import get_mysql_engine
from weather_usecase.utils.response_handler import handle_response, handle_error
from weather_usecase.utils.db_operations import create_table, check_and_remove_duplicates, insert_data

def write_city_coordinates_to_db(city_coordinates_df):
    task_id = 'write_city_coordinates_to_db'
    try:
        engine = get_mysql_engine()
        print(city_coordinates_df)
        inspector = inspect(engine)
        table_name = 'city_coordinates'
        if table_name not in inspector.get_table_names():
            table_schema = [
                Column('city_id', String(36), primary_key=True),
                Column('city_name', String(255), nullable=False),
                Column('coordinate_id', String(36), nullable=False),
                Column('latitude', Float, nullable=False),
                Column('longitude', Float, nullable=False),
            ]
            create_table(table_name, table_schema, engine)
        city_coordinates_df = check_and_remove_duplicates(city_coordinates_df, table_name, engine, ignore_columns=['city_id', 'coordinate_id'])
        if not insert_data(city_coordinates_df, table_name, engine):
            handle_response(pd.DataFrame(), task_id)
            return
        handle_response(city_coordinates_df, task_id)
    except Exception as e:
        handle_error(e, task_id)
        raise e

def write_weather_to_db(weather_df):
    task_id = 'write_weather_to_db'
    try:
        engine = get_mysql_engine()
        inspector = inspect(engine)
        table_name = 'weather'
        if table_name not in inspector.get_table_names():
            table_schema = [
                Column('weather_id', String(36), primary_key=True),
                Column('city_id', String(36), ForeignKey('city_coordinates.city_id'), nullable=False),
                Column('temperature', Float, nullable=False),
                Column('humidity', Float, nullable=False),
                Column('precipitation', Float, nullable=False),
                Column('wind_speed', Float, nullable=False),
                Column('cloud_cover', Float, nullable=False),
                Column('particular_date', Date, nullable=False),
                Column('time', Time, nullable=False),
            ]
            create_table(table_name, table_schema, engine)
        weather_df = check_and_remove_duplicates(weather_df, table_name, engine, ignore_columns=['weather_id'])
        if not insert_data(weather_df, table_name, engine):
            handle_response(pd.DataFrame(), task_id)
            return
        handle_response(weather_df, task_id)
    except Exception as e:
        handle_error(e, task_id)
        raise e

def write_uv_index_to_db(uv_index_df):
    task_id = 'write_uv_index_to_db'
    try:
        engine = get_mysql_engine()
        inspector = inspect(engine)
        table_name = 'uv_index'
        if table_name not in inspector.get_table_names():
            table_schema = [
                Column('uv_id', String(36), primary_key=True),
                Column('city_id', String(36), ForeignKey('city_coordinates.city_id'), nullable=False),
                Column('uv_index_value', Float, nullable=False),
                Column('particular_date', Date, nullable=False),
                Column('time', Time, nullable=False),
            ]
            create_table(table_name, table_schema, engine)
        uv_index_df = check_and_remove_duplicates(uv_index_df, table_name, engine, ignore_columns=['uv_id'])
        if not insert_data(uv_index_df, table_name, engine):
            handle_response(pd.DataFrame(), task_id)
            return
        handle_response(uv_index_df, task_id)
    except Exception as e:
        handle_error(e, task_id)
        raise e

def write_air_quality_index_to_db(aq_index_df):
    task_id = 'write_air_quality_index_to_db'
    try:
        engine = get_mysql_engine()
        inspector = inspect(engine)
        table_name = 'air_quality_index'
        if table_name not in inspector.get_table_names():
            table_schema = [
                Column('aq_id', String(36), primary_key=True),
                Column('city_id', String(36), ForeignKey('city_coordinates.city_id'), nullable=False),
                Column('pm10', Float, nullable=False),
                Column('pm2_5', Float, nullable=False),
                Column('o3', Float, nullable=False),
                Column('no2', Float, nullable=False),
                Column('so2', Float, nullable=False),
                Column('co', Float, nullable=False),
                Column('particular_date', Date, nullable=False),
                Column('time', Time, nullable=False),
            ]
            create_table(table_name, table_schema, engine)
        aq_index_df = check_and_remove_duplicates(aq_index_df, table_name, engine, ignore_columns=['aq_id'])
        if not insert_data(aq_index_df, table_name, engine):
            handle_response(pd.DataFrame(), task_id)
            return
        handle_response(aq_index_df, task_id)
    except Exception as e:
        handle_error(e, task_id)
        raise e

def write_historical_temperature_to_db(historical_temperature_df):
    task_id = 'write_historical_temperature_to_db'
    try:
        engine = get_mysql_engine()
        inspector = inspect(engine)
        table_name = 'historical_temperature'
        if table_name not in inspector.get_table_names():
            table_schema = [
                Column('temp_id', String(36), primary_key=True),
                Column('city_id', String(36), ForeignKey('city_coordinates.city_id'), nullable=False),
                Column('temperature', Float, nullable=False),
                Column('particular_date', Date, nullable=False),
                Column('time', Time, nullable=False),
            ]
            create_table(table_name, table_schema, engine)
        historical_temperature_df = check_and_remove_duplicates(historical_temperature_df, table_name, engine, ignore_columns=['temp_id'])
        if not insert_data(historical_temperature_df, table_name, engine):
            handle_response(pd.DataFrame(), task_id)
            return
        handle_response(historical_temperature_df, task_id)
    except Exception as e:
        handle_error(e, task_id)
        raise e

def write_historical_precipitation_to_db(historical_precipitation_df):
    task_id = 'write_historical_precipitation_to_db'
    try:
        engine = get_mysql_engine()
        inspector = inspect(engine)
        table_name = 'historical_precipitation'
        if table_name not in inspector.get_table_names():
            table_schema = [
                Column('precip_id', String(36), primary_key=True),
                Column('city_id', String(36), ForeignKey('city_coordinates.city_id'), nullable=False),
                Column('precipitation', Float, nullable=False),
                Column('particular_date', Date, nullable=False),
                Column('time', Time, nullable=False),
            ]
            create_table(table_name, table_schema, engine)
        historical_precipitation_df = check_and_remove_duplicates(historical_precipitation_df, table_name, engine, ignore_columns=['precip_id'])
        if not insert_data(historical_precipitation_df, table_name, engine):
            handle_response(pd.DataFrame(), task_id)
            return
        handle_response(historical_precipitation_df, task_id)
    except Exception as e:
        handle_error(e, task_id)
        raise e