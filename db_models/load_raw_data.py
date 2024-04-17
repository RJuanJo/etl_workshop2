import psycopg2
import pandas as pd
import json

def load_spotify_data(filepath):
    with open('config\credentials.json', 'r') as file:
        credentials = json.load(file)
    
    dataframe = pd.read_csv(filepath)
    conn = psycopg2.connect(
        dbname='workshop_2',
        user=credentials['user'],
        password=credentials['password'],
        host=credentials['host'],
        port=credentials['port']
    )
    cursor = conn.cursor()

    # SQL Sentence
    insert_query = """
    INSERT INTO spotify_data (
        unnamed_0, track_id, artists, album_name, track_name, popularity,
        duration_ms, explicit, danceability, energy, key, loudness, mode,
        speechiness, acousticness, instrumentalness, liveness, valence,
        tempo, time_signature, track_genre
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """


    try:
        cursor.executemany(insert_query, dataframe.values.tolist())
        conn.commit()
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

filepath = 'data\spotify_dataset.csv'
load_spotify_data(filepath)


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