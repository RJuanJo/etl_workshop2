import pandas as pd
import numpy as np
import json
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def merge_data(**kwargs):
    logger = logging.getLogger(__name__)
    logger.info("Starting Merge")

    ti = kwargs["ti"]
    
    try:
        df_grammys = json.loads(ti.xcom_pull(task_ids='transform_grammy_data_task'))
        df_spotify = json.loads(ti.xcom_pull(task_ids='transform_spotify'))
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON: {e}")
        raise

    try:
        df_grammys = pd.json_normalize(data=df_grammys)
        df_spotify = pd.json_normalize(data=df_spotify)
    except Exception as e:
        logger.error(f"Error normalizing JSON data: {e}")
        raise

    try:
        df_merged = pd.merge(df_grammys, df_spotify, on=["artists", "track_name"], how="outer")
        logger.info("DataFrames merged successfully.")
    except KeyError as e:
        logger.error(f"Error during merge: {e}")
        raise

    
    df_merged['winner'].fillna(False, inplace=True)
    df_merged['awards_group'].fillna('Not in Group', inplace=True)
    df_merged['title_by_year'].fillna('No Title', inplace=True)
    logger.info("Null values filled.")

   
    logger.info(f"Final Data: {df_merged.info()}")
    return df_merged.to_json(orient='records')


def get_engine():
    with open('/config/credentials.json', 'r') as json_file:
        credentials = json.load(json_file)
    engine = create_engine(f"postgresql://{credentials['user']}:{credentials['password']}@host.docker.internal:{credentials['port']}/{credentials['database']}")
    return engine

def save_to_postgres(**kwargs):
    logger = logging.getLogger(__name__)
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='merge_data')

    try:
        df = pd.read_json(json_data, orient='records')
        logger.info("Data converted to DataFrame successfully")
    except Exception as e:
        logger.error(f"Failed to convert JSON to DataFrame: {e}")
        raise

    try:
        with open('/config/credentials.json', 'r') as json_file:
            data = json.load(json_file)
        engine = get_engine()
        df.to_sql('merged_data', con=engine, if_exists='replace', index=False)
        logger.info("Data saved to PostgreSQL successfully")
    except Exception as e:
        logger.error(f"Failed to save data to PostgreSQL: {e}")
        raise