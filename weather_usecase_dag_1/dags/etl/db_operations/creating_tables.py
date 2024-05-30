import sqlite3
import os
from dotenv import load_dotenv

load_dotenv()

DB_NAME = os.getenv('DB_NAME')

current_directory = os.path.dirname(os.path.abspath(__file__))
database_path = os.path.join(current_directory, DB_NAME)

def create_tables_sqlite():

    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS weather')
    cursor.execute('DROP TABLE IF EXISTS uv_index')
    cursor.execute('DROP TABLE IF EXISTS air_quality')
    cursor.execute('DROP TABLE IF EXISTS hist_temperature')
    cursor.execute('DROP TABLE IF EXISTS hist_precipitation')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS cities (
        city_id TEXT PRIMARY KEY,
        city_name TEXT NOT NULL
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS coordinates (
        coord_id TEXT PRIMARY KEY,
        city_id TEXT,
        latitude REAL NOT NULL,
        longitude REAL NOT NULL,
        FOREIGN KEY (city_id) REFERENCES cities(city_id)
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS weather (
        weather_id TEXT PRIMARY KEY,
        coordinate_uu_id TEXT,
        date TEXT NOT NULL,
        temperature REAL,
        precipitation REAL,
        humidity REAL,
        cloud_cover REAL,
        wind_speed REAL,
        FOREIGN KEY (coordinate_uu_id) REFERENCES coordinates(coord_id)
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS uv_index (
        uvIndex_id TEXT PRIMARY KEY,
        coordinate_uu_id TEXT,
        date TEXT NOT NULL,
        uv_index REAL,
        uv_index_clear_sky REAL,
        FOREIGN KEY (coordinate_uu_id) REFERENCES coordinates(coord_id)
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS air_quality (
        aqi_id TEXT PRIMARY KEY,
        coordinate_uu_id TEXT,
        date TEXT NOT NULL,    
        aqi REAL,
        aqi_PM_2_5 REAL,
        aqi_PM_10 REAL,
        aqi_NO2 REAL,
        aqi_CO REAL,
        aqi_O3 REAL,
        aqi_SO2 REAL,
        FOREIGN KEY (coordinate_uu_id) REFERENCES coordinates(coord_id)
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS hist_temperature (
        hist_temp_id TEXT PRIMARY KEY,
        coordinate_uu_id TEXT,
        date TEXT NOT NULL,
        temperature REAL,
        FOREIGN KEY (coordinate_uu_id) REFERENCES coordinates(coord_id)
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS hist_precipitation (
        hist_preci_id TEXT PRIMARY KEY,
        coordinate_uu_id TEXT,
        date TEXT NOT NULL,
        precipitation REAL,
        snowfall REAL,
        rain REAL,
        FOREIGN KEY (coordinate_uu_id) REFERENCES coordinates(coord_id)
    )
    ''')

    conn.commit()
    conn.close()
