CREATE TABLE movies (
    id INT IDENTITY(1,1) PRIMARY KEY, -- Campo autoincremental para mantener un orden claro
    movie_id INT NOT NULL UNIQUE, -- ID �nico de la pel�cula proporcionado por TMDB
    title NVARCHAR(255) NOT NULL, -- T�tulo de la pel�cula
    release_date DATE, -- Fecha de lanzamiento
    original_language NVARCHAR(10), -- Idioma original
    vote_average FLOAT, -- Promedio de votos
    vote_count INT, -- N�mero de votos
    popularity FLOAT, -- Popularidad num�rica
    overview NVARCHAR(MAX), -- Resumen o descripci�n de la pel�cula
    genre_ids NVARCHAR(MAX) -- Lista de g�neros en formato CSV
);

CREATE TABLE movies_popularity (
    id INT IDENTITY(1,1) PRIMARY KEY, -- Campo autoincremental
    movie_id INT NOT NULL, -- Relaci�n con la tabla movies
    popularity_category NVARCHAR(50) NOT NULL, -- Categor�a de popularidad (Alta, Media, Baja)
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id) -- Llave for�nea hacia 'movies'
);


SELECT * FROM movies
GO
SELECT * FROM movies_popularity