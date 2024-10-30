import os
import requests
from bs4 import BeautifulSoup
import psycopg2
from dotenv import load_dotenv
import time

load_dotenv()

db_host = os.getenv('DB_HOST')
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASS')
api_key = os.getenv('API_KEY')
access_token = os.getenv('ACCESS_TOKEN')

conn = psycopg2.connect(
    host=db_host,
    database=db_name,
    user=db_user,
    password=db_pass
)
c = conn.cursor()


c.execute('''
CREATE TABLE IF NOT EXISTS actors (
    id SERIAL PRIMARY KEY,
    name TEXT,
    profile_path TEXT,
    character TEXT,
    movie_title TEXT
)
''')

def get_actor_movies(actor_id):
    url = f'https://api.themoviedb.org/3/person/{actor_id}/movie_credits'
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    params = {
        'api_key': api_key
    }
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        data = response.json()
        return data['cast']
    else:
        print(f'Error: {response.status_code}')
        return []

def scrape_and_store_data():

    url = 'https://api.themoviedb.org/3/movie/27205/credits'
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    params = {
        'api_key': api_key
    }

    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        data = response.json()
        for actor in data['cast']:
            name = actor['name']
            profile_path = actor['profile_path']
            character = actor['character']
            actor_id = actor['id']
            
            movies = get_actor_movies(actor_id)
            for movie in movies:
                movie_title = movie['title']
                
        
                c.execute('''
                INSERT INTO actors (name, profile_path, character, movie_title) VALUES (%s, %s, %s, %s)
                ''', (name, profile_path, character, movie_title))

                print(f'Nombre: {name}')
                print(f'Imagen: {profile_path}')
                print(f'Personaje: {character}')
                print(f'Película: {movie_title}')
                print('---')
    else:
        print(f'Error: {response.status_code}')

    conn.commit()

try:
    while True:
        scrape_and_store_data()
        time.sleep(200) 
except KeyboardInterrupt:
    print("Proceso detenido manualmente.")
finally:
    conn.close()
