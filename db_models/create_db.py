import psycopg2
import json

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

    # Create Table spotify_data
    cursor.execute("""
        CREATE TABLE spotify_data (
            unnamed_0 INT,
            track_id VARCHAR(1000),
            artists VARCHAR(1000),
            album_name VARCHAR(1000),
            track_name VARCHAR(1000),
            popularity INT,
            duration_ms INT,
            explicit BOOLEAN,
            danceability FLOAT,
            energy FLOAT,
            key INT,
            loudness FLOAT,
            mode INT,
            speechiness FLOAT,
            acousticness FLOAT,
            instrumentalness FLOAT,
            liveness FLOAT,
            valence FLOAT,
            tempo FLOAT,
            time_signature INT,
            track_genre VARCHAR(1000)
        );
    """)
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
