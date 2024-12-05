import os
import json
import requests
import pandas as pd
import pyodbc
import datetime
from sqlalchemy import text
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()
api_token = os.getenv("TOKEN")
api_key = os.getenv("KEY")
db_url = os.getenv("DB")
engine = create_engine(db_url)

def extract_popular_movies(token):
        popular = f'https://api.themoviedb.org/3/movie/popular?language=en-US&page=1'
        url = popular
        headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_token}"}

        response = requests.get(url, headers= headers)
        if response.status_code == 200:
                movies = response.json()
                #print(movies)
                return movies
        else:
            print(f'error al extraer los datos: {response.status_code}')
            return None
#extract_popular_movies(api_token)

def transform_movies(movies):
    if movies is not None:
        results= movies['results']

        df_movies = pd.DataFrame([{
            "movie_id": movie.get('id'),
            "title": movie.get('title'),
            "release_date": movie.get('release_date'),
            "original_language": movie.get('original_language'),
            "vote_average": movie.get('vote_average'),
            "vote_count": movie.get('vote_count'),
            "popularity": movie.get('popularity'),
            "overview": movie.get('overview'),
            "genre_ids": ",".join(map(str, movie.get('genre_ids', []))) #["20", "10", "00"] --- "20,10,00"       #movie.get(...)devuelve el diccionario. con map(str, ) convierte la salida anterior en texto. y con el ",".join(..) los separo con comas.
        } for movie in results]) #para cada pelicula de la lista de resultados

        #inserto una nueva fila y dependiendo de la popularidad se asigna baja, media o alta
        print('creando la nueva fila de popularidad...')
        df_movies['popularity_category'] = pd.cut(
            df_movies['popularity'],
            bins=[-float('inf'), 1000, 3000, float('inf')], #si es menor a 1000 es bajo, si es mayor a 3000 es alto y si esta entre esos, es media
            labels=['Baja', 'Media', 'Alta']
        )
        return df_movies
       
    else:
        print("Error al transformar los datos", df_movies)


def load_to_sql(df_movies, engine):
    if df_movies is not None:
        try:
            with engine.connect() as connection:
                for _, row in df_movies.iterrows():
                    row = row.copy()
                    
                    #reemplazo los valores null por None para no tener errores de datos
                    if pd.isnull(row['release_date']):
                        row['release_date'] = pd.NaT  #cambio las fechas nulas

                    row_dict = row.to_dict() #cambio el pandas a un diccionario de python, para poder hacer .execute luego
                    
                    #print(row_dict) #print para debuggear :)

                    #armo el UPSERT para insertar/actualizar las peliculas en la tabla movies
                    UPSERT_MOVIES = text("""
                        MERGE INTO movies AS target
                        USING (SELECT :movie_id AS movie_id,
                                      :title AS title,
                                      :release_date AS release_date,
                                      :original_language AS original_language,
                                      :vote_average AS vote_average,
                                      :vote_count AS vote_count,
                                      :popularity AS popularity,
                                      :overview AS overview,
                                      :genre_ids AS genre_ids) AS source
                        ON target.movie_id = source.movie_id
                        WHEN MATCHED THEN
                            UPDATE SET 
                                title = source.title,
                                release_date = source.release_date,
                                original_language = source.original_language,
                                vote_average = source.vote_average,
                                vote_count = source.vote_count,
                                popularity = source.popularity,
                                overview = source.overview,
                                genre_ids = source.genre_ids
                        WHEN NOT MATCHED THEN
                            INSERT (movie_id, title, release_date, original_language, vote_average, vote_count, popularity, overview, genre_ids)
                            VALUES (source.movie_id, source.title, source.release_date, source.original_language, source.vote_average, source.vote_count, source.popularity, source.overview, source.genre_ids);
                    """)
                    
                    #ejecuto el UPSERT en la tabla movies
                    connection.execute(UPSERT_MOVIES, row_dict)
                    
                    #armo la query UPSERT para comparar los datos y evitar duplicados
                    UPSERT_POPULARITY = text("""
                        MERGE INTO movies_popularity AS target
                        USING (SELECT :movie_id AS movie_id,
                                      :popularity_category AS popularity_category) AS source
                        ON target.movie_id = source.movie_id
                        WHEN MATCHED THEN
                            UPDATE SET 
                                popularity_category = source.popularity_category
                        WHEN NOT MATCHED THEN
                            INSERT (movie_id, popularity_category)
                            VALUES (source.movie_id, source.popularity_category);
                    """)
                    
                    #ejecuto el UPSERT a la tabla popularity
                    connection.execute(UPSERT_POPULARITY, {
                        "movie_id": row_dict['movie_id'],
                        "popularity_category": row_dict['popularity_category']
                    })
                    
                    #commit para guardar los datos
                    connection.commit()
                    print('Proceso completado, datos cargados/Actualizados a la base de datos.')
                    
        except Exception as e:
            print(f"Error al cargar los datos a SQL: {str(e)}")
            return None

def etl():
    print(f'** Extrayendo las peliculas mas populares de la API... **')
    raw_data = extract_popular_movies(api_token)
    if raw_data is not None:
        print('** Datos extraidos correctamente')
        print(f'\n** Transformando datos...')
        transformed_data = transform_movies(raw_data)
    if transformed_data is not None:
        print('\nDatos transformados correctamente')
        print('Cargando datos a las tablas...')
        load_to_sql(transformed_data, engine)
        input('************')

if __name__ == "__main__":
        etl()

'''{'adult': False, 'backdrop_path': '/tElnmtQ6yz1PjN1kePNl8yMSb59.jpg', 'genre_ids': [16, 12, 10751, 35], 'id': 1241982, 'original_language': 'en', 'original_title': 'Moana 2', 'overview': "After receiving an unexpected call from her wayfinding ancestors, Moana journeys alongside Maui and a new crew to the far seas of Oceania and into dangerous, long-lost waters for an adventure unlike anything she's ever faced.", 'popularity': 6106.764, 'poster_path': '/yh64qw9mgXBvlaWDi7Q9tpUBAvH.jpg', 'release_date': '2024-11-27', 'title': 'Moana 2', 'video': False, 'vote_average': 7.0, 'vote_count': 291},'''