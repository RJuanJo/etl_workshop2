import pandas as pd
import logging
import json
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
import numpy as np

def extract_spotify_data(**kwargs):
    logger = logging.getLogger(__name__)
    logger.info('Starting extraction of Spotify dataset from CSV')

    try:
        df_spotify = pd.read_csv('/data/spotify_dataset.csv')
        #df_spotify = df_spotify.iloc[:50000] 

        logger.info('Spotify dataset loaded successfully', df_spotify.info())

        return df_spotify.to_json(orient='records')
    except Exception as e:
        logger.error('Failed to load Spotify dataset', exc_info=True)
        raise

def transform_spotify_data(**kwargs):
    logger = logging.getLogger(__name__)
    logger.info('Starting transformation of Spotify data')

    try:

        df_spotify = pd.read_json(kwargs.get('df_spotify'))
        df_spotify = df_spotify.drop_duplicates(subset='track_name', keep='first')
        logger.info(f'Removed duplicates - remaining records: {len(df_spotify)}')

        df_spotify = df_spotify[['track_name', 'artists']]
        logger.info('Filtered required columns', df_spotify.head())


        return df_spotify.to_json(orient='records')
    except Exception as e:
        logger.error('Failed during the transformation of Spotify data', exc_info=True)
        raise