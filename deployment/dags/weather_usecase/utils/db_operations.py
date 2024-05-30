import pandas as pd
from sqlalchemy import inspect, create_engine, MetaData, Table

from weather_usecase.utils.response_handler import handle_error

def create_table(table_name, table_schema, engine):
    metadata = MetaData()
    table = Table(table_name, metadata, *table_schema)
    metadata.create_all(engine, checkfirst=True)

def check_and_remove_duplicates(df, table_name, engine, ignore_columns=[]):
    existing_data = pd.read_sql_table(table_name, engine)
    columns_to_check = [col for col in existing_data.columns if col not in ignore_columns]
    df = df[~df[columns_to_check].isin(existing_data[columns_to_check]).all(axis=1)]
    return df

def insert_data(df, table_name, engine):
    try:
        df.to_sql(table_name, con=engine, if_exists='append', index=False)
    except Exception as e:
        handle_error(e, f'insert_data_{table_name}')
        return False
    return True