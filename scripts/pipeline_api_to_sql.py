import os
import json
import requests
import pandas as pd
import pyodbc
from sqlalchemy import text
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()
api_token = os.getenv("TOKEN")
api_key = os.getenv("KEY")
popular = "https://api.themoviedb.org/3/movie/popular?language=en-US&page=1"

def extract_popular_movies(token):

        url = popular
        headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_token}"}

        response = requests.get(url, headers= headers)
        if response.status_code == 200:
                movies = response.json()
                print('datos extraidos correctamente')
                #print(movies)
                return movies
        else:
            print(f'error al extraer los datos: {response.status_code}')
            return None
#extract_popular_movies(api_token)

def transform_movies(movies):
       if movies is not None:

        results = movies['results']

        df_movies = pd.DataFrame([{
            "movie_id": movie.get('id'),
            "title": movie.get('title'),
            "release_date": movie.get('release_date'),
            "original_languaje": movie.get('original_languaje'),
            "vote_average": movie.get('vote_average'),
            "vote_count": movie.get('vote_count'),
            "popularity": movie.get('popularity'),
            "overview": movie.get('overview'),
            "genre_ids": ",".join(map(str, movie.get('genre_ids', [])))

        } for movie in results])

        # Clasifica la popularidad
        df_movies['popularity_category'] = pd.cut(
            df_movies['popularity'],
            bins=[-float('inf'), 100, 500, float('inf')],
            labels=['Baja', 'Media', 'Alta']
        )
        print("datos transformados a dataframe correctamente")
        return df_movies


def etl():
    movies = extract_popular_movies(api_token)
    df_movies = transform_movies(movies)
    if df_movies is not None:
        print('FIN')
        print(df_movies)
    else:
         print('ERROR')
etl()
'''{'adult': False, 'backdrop_path': '/tElnmtQ6yz1PjN1kePNl8yMSb59.jpg', 'genre_ids': [16, 12, 10751, 35], 'id': 1241982, 'original_language': 'en', 'original_title': 'Moana 2', 'overview': "After receiving an unexpected call from her wayfinding ancestors, Moana journeys alongside Maui and a new crew to the far seas of Oceania and into dangerous, long-lost waters for an adventure unlike anything she's ever faced.", 'popularity': 6106.764, 'poster_path': '/yh64qw9mgXBvlaWDi7Q9tpUBAvH.jpg', 'release_date': '2024-11-27', 'title': 'Moana 2', 'video': False, 'vote_average': 7.0, 'vote_count': 291},'''
#