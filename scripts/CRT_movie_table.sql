CREATE TABLE movies (
    id INT IDENTITY(1,1) PRIMARY KEY, -- Campo autoincremental para mantener un orden claro
    movie_id INT NOT NULL UNIQUE, -- ID único de la película proporcionado por TMDB
    title NVARCHAR(255) NOT NULL, -- Título de la película
    release_date DATE, -- Fecha de lanzamiento
    original_language NVARCHAR(10), -- Idioma original
    vote_average FLOAT, -- Promedio de votos
    vote_count INT, -- Número de votos
    popularity FLOAT, -- Popularidad numérica
    overview NVARCHAR(MAX), -- Resumen o descripción de la película
    genre_ids NVARCHAR(MAX) -- Lista de géneros en formato CSV
);

CREATE TABLE movies_popularity (
    id INT IDENTITY(1,1) PRIMARY KEY, -- Campo autoincremental
    movie_id INT NOT NULL, -- Relación con la tabla movies
    popularity_category NVARCHAR(50) NOT NULL, -- Categoría de popularidad (Alta, Media, Baja)
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id) -- Llave foránea hacia 'movies'
);


SELECT * FROM movies
GO
SELECT * FROM movies_popularity