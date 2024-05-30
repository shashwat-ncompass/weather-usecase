from etl.extract_functions import (
    extract_cities_coordinates, extract_weather, extract_uv_index,
    extract_air_quality_index, extract_historical_temperature, extract_historical_precipitation
)
from etl.transform_functions import (
    transform_city_coordinates, transform_weather, transform_uv_index,
    transform_air_quality_index, transform_historical_temperature, transform_historical_precipitation
)
from etl.aggregate_functions import (
    aggregate_extracted_data, aggregate_transformed_data
)
from etl.load_functions import (
    write_city_coordinates_to_db, write_weather_to_db, write_uv_index_to_db,
    write_air_quality_index_to_db, write_historical_temperature_to_db, write_historical_precipitation_to_db
)

def main():
    try:
        cities_df, coordinates_df = extract_cities_coordinates()
        weather_df = extract_weather()
        uv_index_df = extract_uv_index()
        air_quality_index_df = extract_air_quality_index()
        historical_temperature_df = extract_historical_temperature()
        historical_precipitation_df = extract_historical_precipitation()
        
        extracted_data = aggregate_extracted_data(
            cities_df, coordinates_df, weather_df, uv_index_df,
            air_quality_index_df, historical_temperature_df, historical_precipitation_df
        )


        city_coordinates_df = transform_city_coordinates(extracted_data[0], extracted_data[1])
        weather_df = transform_weather(extracted_data[2])
        uv_index_df = transform_uv_index(extracted_data[3])
        air_quality_index_df = transform_air_quality_index(extracted_data[4])
        historical_temperature_df = transform_historical_temperature(extracted_data[5])
        historical_precipitation_df = transform_historical_precipitation(extracted_data[6])

        transformed_data = aggregate_transformed_data(
            city_coordinates_df, weather_df, uv_index_df,
            air_quality_index_df, historical_temperature_df, historical_precipitation_df
        )

        for idx, df in enumerate(transformed_data):
            print(f"Columns in DataFrame {idx}: {df.columns.tolist()}")

        write_city_coordinates_to_db(transformed_data[0])
        write_weather_to_db(transformed_data[1])
        write_uv_index_to_db(transformed_data[2])
        write_air_quality_index_to_db(transformed_data[3])
        write_historical_temperature_to_db(transformed_data[4])
        write_historical_precipitation_to_db(transformed_data[5])

        print("ETL process completed successfully!")
    except Exception as e:
        print(f"An error occurred during the ETL process: {e}")

if __name__ == "__main__":
    main()