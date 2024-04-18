import logging
import pandas as pd
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
import json
import numpy as np

def extract_grammy_data(**kwargs):
    logger = logging.getLogger(__name__)
    logger.info("Starting Grammy data extraction")

    with open('/config/credentials.json', 'r') as json_file:
        data = json.load(json_file)

    engine = create_engine(f"postgresql://{data['user']}:{data['password']}@host.docker.internal:{data['port']}/{data['database']}")
    session = sessionmaker(bind=engine)()

    try:
        metadata = MetaData(bind=engine)
        metadata.reflect()
        grammys_table = metadata.tables['grammys_data']
        grammys_records = session.query(grammys_table).all()
        grammys_data = [record._asdict() for record in grammys_records]
        df_grammys = pd.DataFrame(grammys_data)
        logger.info("Grammy data extraction completed", df_grammys.info())
    finally:
        session.close()
        logger.info("Database session closed")

    return df_grammys.to_json(orient='records')

def transform_grammy_data(**kwargs):
    logger = logging.getLogger(__name__)
    logger.info("Starting Grammy data transformation")


    str_data = kwargs.get('df_grammys')
    df_grammys = pd.read_json(str_data)

    df_grammys.replace('NaN', np.nan, inplace=True)
    df_grammys.rename(columns={'artist': 'artists', 'nominee': 'track_name'}, inplace=True)
    df_grammys['awards_group'] = df_grammys['category'].str.extract('^(Song Of The Year|Record Of The Year|Album Of The Year)')
    df_grammys.loc[df_grammys['category'].str.startswith('Best'), 'awards_group'] = 'Excellence Awards'
    df_grammys['awards_group'] = df_grammys['awards_group'].fillna('Excellence Awards')
    df_grammys['year_from_title'] = df_grammys['title'].str.extract('(\d{4})').astype(int)

    bins = [1957, 1970, 1999, 2019]
    labels = ['(1958-1970) AGM', '(1971-1999) AGM', '(2000-2019) AGM']
    df_grammys['title_by_year'] = pd.cut(df_grammys['year_from_title'].astype(int), bins=bins, labels=labels, right=True)

    df_grammys.drop(['img', 'workers', 'published_at', 'updated_at', 'year', 'title', 'category','year_from_title'], axis=1, inplace=True)
    df_grammys['artists'] = df_grammys['artists'].fillna('No record')

    logger.info("Grammy data transformation completed", df_grammys.head())
    return df_grammys.to_json(orient='records')
