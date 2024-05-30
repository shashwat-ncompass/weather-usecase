import sqlite3
import json
import uuid
from db_operations import queries
import os
from dotenv import load_dotenv

load_dotenv()

DB_NAME = os.getenv('DB_NAME')

current_directory = os.path.dirname(os.path.abspath(__file__))
database_path = os.path.join(current_directory, DB_NAME)

def insert_city_data():

    json_file_path = os.path.join(current_directory, 'cities.json')

    with open(json_file_path, 'r', encoding='utf-8') as f:
        city_data = json.load(f)

    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    for city in city_data:
        city_name = city['city']
        lat = float(city['lat'])
        lng = float(city['lng'])
        city_uu_id = str(uuid.uuid4())
        coord_uu_id = str(uuid.uuid4())

        cursor.execute(queries.insert_cities, (city_uu_id, city_name))

        cursor.execute(queries.insert_coordinates, (coord_uu_id, city_uu_id, lat, lng))

    conn.commit()
    conn.close()


