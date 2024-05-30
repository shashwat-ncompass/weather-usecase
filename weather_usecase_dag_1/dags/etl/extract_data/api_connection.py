import openmeteo_requests
import requests_cache
from retry_requests import retry

cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

def fetch_air_quality_data(lat, lng):
    url = "https://air-quality-api.open-meteo.com/v1/air-quality"
    params = {
        "latitude": lat,
        "longitude": lng,
        "hourly": ["us_aqi", "us_aqi_pm2_5", "us_aqi_pm10", "us_aqi_nitrogen_dioxide", "us_aqi_carbon_monoxide", "us_aqi_ozone", "us_aqi_sulphur_dioxide"],
        "forecast_days": 1
    }
    return openmeteo.weather_api(url, params=params)

def fetch_weather_data(lat, lng):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lng,
        "hourly": ["relative_humidity_2m", "precipitation", "cloud_cover", "wind_speed_180m", "temperature_180m", "uv_index", "uv_index_clear_sky"],
        "forecast_days": 1
    }
    return openmeteo.weather_api(url, params=params)

def fetch_historical_data(lat, lng):
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lng,
        "start_date": "2023-01-01",
        "end_date": "2023-03-01",
        "hourly": ["temperature_2m", "precipitation", "rain", "snowfall"]
    }
    return openmeteo.weather_api(url, params=params)