import os
import sqlite3
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

def get_weather_sqlite_connection():
    weather_db_name = os.getenv('WEATHER_DB_NAME')
    conn = sqlite3.connect(weather_db_name)
    return conn

def get_historical_sqlite_connection():
    historical_db_name = os.getenv('HISTORICAL_DB_NAME')
    conn = sqlite3.connect(historical_db_name)
    return conn

def get_mysql_engine():
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT')
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    db = os.getenv('DB_DATABASE')
    
    engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db}')
    return engine