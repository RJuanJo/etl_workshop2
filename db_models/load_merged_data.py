import pandas as pd
import psycopg2
import json


with open('config/credentials.json', 'r') as file:
    credentials = json.load(file)

conn_str = f"dbname='{credentials['database']}' user='{credentials['user']}' host='{credentials['host']}' password='{credentials['password']}' port='{credentials['port']}'"

conn = psycopg2.connect(conn_str)

# Crete Table merged_data
cursor = conn.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS merged_data (
        track_name TEXT,
        artists TEXT,
        winner TEXT,
        awards_group TEXT,
        title_by_year TEXT
    );
''')
conn.commit()

with open('data/merged_data.csv', 'r', encoding='utf-8') as f:
    next(f) 
    cursor.copy_expert("COPY merged_data FROM STDIN WITH CSV HEADER DELIMITER ',' NULL '\\N';", f)
conn.commit()
