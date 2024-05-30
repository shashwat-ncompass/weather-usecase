CREATE DATABASE IF NOT EXISTS weather_usecase;
USE weather_usecase;

-- city_coordinates table
CREATE TABLE IF NOT EXISTS city_coordinates (
    city_id VARCHAR(36) PRIMARY KEY,
    city_name VARCHAR(255),
    coordinate_id VARCHAR(36),
    latitude FLOAT,
    longitude FLOAT
);

-- weather table
CREATE TABLE IF NOT EXISTS weather (
    weather_id VARCHAR(36) PRIMARY KEY,
    city_id VARCHAR(36),
    temperature FLOAT,
    humidity FLOAT,
    precipitation FLOAT,
    wind_speed FLOAT,
    cloud_cover FLOAT,
    particular_date DATE,
    time TIME,
    FOREIGN KEY (city_id) REFERENCES city_coordinates(city_id)
);

-- uv_index table
CREATE TABLE IF NOT EXISTS uv_index (
    uv_id VARCHAR(36) PRIMARY KEY,
    city_id VARCHAR(36),
    uv_index FLOAT,
    uv_index_clear_sky FLOAT,
    particular_date DATE,
    time TIME,
    FOREIGN KEY (city_id) REFERENCES city_coordinates(city_id)
);

-- air_quality_index table
CREATE TABLE IF NOT EXISTS air_quality_index (
    aq_id VARCHAR(36) PRIMARY KEY,
    city_id VARCHAR(36),
    pm10 FLOAT,
    pm2_5 FLOAT,
    carbon_monoxide FLOAT,
    nitrogen_dioxide FLOAT,
    sulphur_dioxide FLOAT,
    ozone FLOAT,
    particular_date DATE,
    time TIME,
    FOREIGN KEY (city_id) REFERENCES city_coordinates(city_id)
);

-- historical_temperature table
CREATE TABLE IF NOT EXISTS historical_temperature (
    hist_temp_id VARCHAR(36) PRIMARY KEY,
    coordinate_uu_id VARCHAR(36),
    temperature FLOAT,
    particular_date DATE,
    time TIME
);

-- historical_precipitation table
CREATE TABLE IF NOT EXISTS historical_precipitation (
    hist_preci_id VARCHAR(36) PRIMARY KEY,
    coordinate_uu_id VARCHAR(36),
    precipitation FLOAT,
    snowfall FLOAT,
    rain FLOAT,
    particular_date DATE,
    time TIME
);