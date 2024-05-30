import os
import pandas as pd
import logging

os.makedirs('logs', exist_ok=True)

logging.basicConfig(filename='logs/etl.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def handle_response(response, task_id):
    if isinstance(response, pd.DataFrame):
        if not response.empty:
            logging.info(f"Task {task_id} completed successfully.")
        else:
            logging.warning(f"No data found for task {task_id}.")
    elif isinstance(response, tuple):
        if all(isinstance(df, pd.DataFrame) for df in response):
            if not any(df.empty for df in response):
                logging.info(f"Task {task_id} completed successfully.")
            else:
                logging.warning(f"Some data is empty for task {task_id}.")
        else:
            logging.error(f"Invalid response format for task {task_id}.")
    else:
        logging.error(f"Invalid response format for task {task_id}.")


def handle_error(error, task_id):
    logging.error(f"Error occurred in task {task_id}: {error}")