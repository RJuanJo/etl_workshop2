import psycopg2
import json
import pandas as pd

def create_database_and_table():
    
    with open('config\credentials.json', 'r') as file:
        credentials = json.load(file)
    
    # Connection to PostgreSQL
    conn = psycopg2.connect(
        dbname='postgres', 
        user=credentials['user'],
        password=credentials['password'],
        host=credentials['host'],
        port=credentials['port']
    )
    conn.autocommit = True 
    cursor = conn.cursor()

    cursor.execute("CREATE DATABASE workshop_2;")
    conn.close()

    conn = psycopg2.connect(
        dbname='workshop_2',
        user=credentials['user'],
        password=credentials['password'],
        host=credentials['host'],
        port=credentials['port']
    )
    cursor = conn.cursor()


        # Create Table grammys_data
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS grammys_data (
            year INT,
            title TEXT,
            published_at TIMESTAMP,
            updated_at TIMESTAMP,
            category VARCHAR(255),
            nominee VARCHAR(255),
            artist VARCHAR(255),
            workers TEXT,
            img TEXT,
            winner BOOLEAN
        );
    """)

    conn.commit()
    cursor.close()
    conn.close()

create_database_and_table()

def load_grammys_data(csv_path):
    with open('config\credentials.json', 'r') as file:
        credentials = json.load(file)
    
    dataframe = pd.read_csv(csv_path, parse_dates=['published_at', 'updated_at'])

    conn = psycopg2.connect(
        dbname='workshop_2',
        user=credentials['user'],
        password=credentials['password'],
        host=credentials['host'],
        port=credentials['port']
    )
    
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO grammys_data (
        year, title, published_at, updated_at, category, nominee,
        artist, workers, img, winner
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    for row in dataframe.itertuples(index=False, name=None):
        cursor.execute(insert_query, row)


    conn.commit()
    cursor.close()
    conn.close()
load_grammys_data('data/the_grammy_awards.csv')
