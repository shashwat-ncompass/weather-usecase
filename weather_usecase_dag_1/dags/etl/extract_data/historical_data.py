import sqlite3
import pandas as pd
import uuid
import os
import db_operations.queries as queries
from extract_data.api_connection import fetch_historical_data
from dotenv import load_dotenv

load_dotenv()

DB_NAME = os.getenv('DB_NAME')

def get_historical_data():

    root_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

    database_directory = os.path.join(root_directory, 'db_operations')
    database_path = os.path.join(database_directory, DB_NAME)

    db_file = database_path
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    cursor.execute(queries.select_coordinates)
    coordinates = cursor.fetchall()

    for coordinate in coordinates:
        coord_id, city_id, latitude, longitude = coordinate
        
        historical_responses = fetch_historical_data(latitude, longitude)

        hist_response = historical_responses[0]
        print(f"Fetching historical data for coordinate ID {coord_id}, Coordinates {hist_response.Latitude()}°N {hist_response.Longitude()}°E")
        
        hist_hourly = hist_response.Hourly()

        for i in range(len(hist_hourly.Variables(0).ValuesAsNumpy())):
            date = pd.to_datetime(hist_hourly.Time(), unit="s", utc=True) + pd.Timedelta(seconds=i * hist_hourly.Interval())
            temperature_2m = float(hist_hourly.Variables(0).ValuesAsNumpy()[i])
            precipitation = float(hist_hourly.Variables(1).ValuesAsNumpy()[i])
            rain = float(hist_hourly.Variables(2).ValuesAsNumpy()[i])
            snowfall = float(hist_hourly.Variables(3).ValuesAsNumpy()[i])

            hist_temp_id = str(uuid.uuid4())
            
            cursor.execute(queries.insert_hist_temp_query, (
                hist_temp_id,
                coord_id,
                date.strftime('%Y-%m-%d %H:%M:%S'),
                temperature_2m
            ))

            hist_preci_id = str(uuid.uuid4())
            cursor.execute(queries.insert_hist_preci_query, (
                hist_preci_id,
                coord_id,
                date.strftime('%Y-%m-%d %H:%M:%S'),
                precipitation,
                rain,
                snowfall
            ))

    conn.commit()
    conn.close()

    print('All historical data inserted successfully into tables')
