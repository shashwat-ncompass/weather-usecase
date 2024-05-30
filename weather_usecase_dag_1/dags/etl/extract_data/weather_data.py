import sqlite3
import pandas as pd
import uuid
import os
import db_operations.queries as queries
from extract_data.api_connection import fetch_weather_data
from dotenv import load_dotenv

load_dotenv()

DB_NAME = os.getenv('DB_NAME')

def get_weather_data():

    root_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

    database_directory = os.path.join(root_directory, 'db_operations')
    database_path = os.path.join(database_directory, DB_NAME)

    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    cursor.execute(queries.select_coordinates)
    coordinates = cursor.fetchall()

    for coordinate in coordinates:
        coord_id, city_id, latitude, longitude = coordinate
        responses = fetch_weather_data(latitude, longitude)

        response = responses[0]
        print(f"Fetching data for coordinate ID {coord_id}, Coordinates {response.Latitude()}°N {response.Longitude()}°E")
        
        hourly = response.Hourly()

        for i in range(len(hourly.Variables(0).ValuesAsNumpy())):
            date = pd.to_datetime(hourly.Time(), unit="s", utc=True) + pd.Timedelta(seconds=i * hourly.Interval())
            relative_humidity_2m = float(hourly.Variables(0).ValuesAsNumpy()[i])
            precipitation = float(hourly.Variables(1).ValuesAsNumpy()[i])
            cloud_cover = float(hourly.Variables(2).ValuesAsNumpy()[i])
            wind_speed_180m = float(hourly.Variables(3).ValuesAsNumpy()[i])
            temperature_180m = float(hourly.Variables(4).ValuesAsNumpy()[i])
            uv_index = float(hourly.Variables(5).ValuesAsNumpy()[i])
            uv_index_clear_sky = float(hourly.Variables(6).ValuesAsNumpy()[i])

            weather_id = str(uuid.uuid4())
            
            cursor.execute(queries.insert_weather_query, (
                weather_id,
                coord_id,
                date.strftime('%Y-%m-%d %H:%M:%S'),
                temperature_180m,
                precipitation,
                relative_humidity_2m,
                cloud_cover,
                wind_speed_180m
            ))

            uv_index_id = str(uuid.uuid4())
            cursor.execute(queries.insert_uv_query, (
                uv_index_id,
                coord_id,
                date.strftime('%Y-%m-%d %H:%M:%S'),
                uv_index,
                uv_index_clear_sky
            ))
        
    print('All data inserted into tables')

    conn.commit()
    conn.close()
