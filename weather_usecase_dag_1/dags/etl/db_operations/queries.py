insert_cities='''INSERT INTO cities (city_id, city_name) VALUES (?, ?)'''
insert_coordinates='''INSERT INTO coordinates (coord_id, city_id, latitude, longitude) VALUES (?, ?, ?, ?)'''

select_coordinates='SELECT coord_id, city_id, latitude, longitude FROM coordinates'

insert_weather_query = '''INSERT INTO weather (weather_id, coordinate_uu_id, date, temperature, precipitation, humidity, cloud_cover, wind_speed)
                                  VALUES (?, ?, ?, ?, ?, ?, ?, ?)'''

insert_uv_query = '''INSERT INTO uv_index (uvIndex_id, coordinate_uu_id, date, uv_index, uv_index_clear_sky)
                             VALUES (?, ?, ?, ?, ?)'''

insert_aqi = ''' INSERT INTO air_quality(aqi_id, coordinate_uu_id,date, aqi,
                            aqi_PM_2_5, aqi_PM_10, aqi_NO2, aqi_CO, aqi_O3, aqi_SO2) VALUES (?,?,?,?,?,?,?,?,?,?)'''

insert_hist_temp_query = '''INSERT INTO hist_temperature (hist_temp_id, coordinate_uu_id, date, temperature)
                                    VALUES (?, ?, ?, ?)'''

insert_hist_preci_query = '''INSERT INTO hist_precipitation (hist_preci_id, coordinate_uu_id, date, precipitation, rain, snowfall)
                                     VALUES (?, ?, ?, ?, ?, ?)'''