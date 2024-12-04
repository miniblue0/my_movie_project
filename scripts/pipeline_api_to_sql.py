import os
import requests
from sqlalchemy import text
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()
api_token = os.getenv("TOKEN")
api_key = os.getenv("KEY")

def extract_data(city_name):
    url = f'https://developer.themoviedb.org/reference/movie-popular-list'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(f"Error al obtener respuesta de la API, status code: {response.status_code}")
        return None