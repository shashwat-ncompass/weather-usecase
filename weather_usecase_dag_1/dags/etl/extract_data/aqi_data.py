import sqlite3
import pandas as pd
import db_operations.queries as queries
import uuid
import os
from extract_data.api_connection import fetch_air_quality_data
from dotenv import load_dotenv

load_dotenv()

DB_NAME = os.getenv('DB_NAME')

def get_aqi_data():

    root_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

    database_directory = os.path.join(root_directory, 'db_operations')
    database_path = os.path.join(database_directory, DB_NAME)

    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    cursor.execute(queries.select_coordinates)
    coordinates = cursor.fetchall()

    for coordinate in coordinates:
        coord_id, city_id, latitude, longitude = coordinate

        try:
            responses_AQI = fetch_air_quality_data(latitude, longitude)
            if not responses_AQI:
                print(f"No air quality data returned for coordinates ({latitude}, {longitude})")
                continue

            response_AQI = responses_AQI[0]
            hourly_AQI = response_AQI.Hourly()
            print(f"Processing air quality data for coordinates ({latitude}, {longitude})")

            for i in range(len(hourly_AQI.Variables(0).ValuesAsNumpy())):
                date = pd.to_datetime(hourly_AQI.Time(), unit="s", utc=True) + pd.Timedelta(seconds=i * hourly_AQI.Interval())
                aqi = float(hourly_AQI.Variables(0).ValuesAsNumpy()[i])
                aqi_PM_2_5 = float(hourly_AQI.Variables(1).ValuesAsNumpy()[i])
                aqi_PM_10 = float(hourly_AQI.Variables(2).ValuesAsNumpy()[i])
                aqi_NO2 = float(hourly_AQI.Variables(3).ValuesAsNumpy()[i])
                aqi_CO = float(hourly_AQI.Variables(4).ValuesAsNumpy()[i])
                aqi_O3 = float(hourly_AQI.Variables(5).ValuesAsNumpy()[i])
                aqi_SO2 = float(hourly_AQI.Variables(6).ValuesAsNumpy()[i])

                aqi_id = str(uuid.uuid4())

                print(f"Inserting AQI data: {aqi_id}, {coord_id}, {date.strftime('%Y-%m-%d %H:%M:%S')}, {aqi}, {aqi_PM_2_5}, {aqi_PM_10}, {aqi_NO2}, {aqi_CO}, {aqi_O3}, {aqi_SO2}")
                
                cursor.execute(queries.insert_aqi, (
                    aqi_id,
                    coord_id,
                    date.strftime('%Y-%m-%d %H:%M:%S'),
                    aqi,
                    aqi_PM_2_5,
                    aqi_PM_10,
                    aqi_NO2,
                    aqi_CO,
                    aqi_O3,
                    aqi_SO2
                ))

        except Exception as e:
            print(f"Error processing air quality data for coordinates ({latitude}, {longitude}): {e}")

    conn.commit()
    conn.close()

    print('All data inserted successfully into tables')


