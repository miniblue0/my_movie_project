CREATE TABLE movies (
    movie_id INT NOT NULL UNIQUE,
    title NVARCHAR(255) NOT NULL, 
    release_date DATE,
    original_language NVARCHAR(10),
    vote_average FLOAT,
    vote_count INT,
    popularity FLOAT,
    overview NVARCHAR(MAX),
    genre_ids NVARCHAR(MAX)
);

CREATE TABLE movies_popularity (
    id INT IDENTITY(1,1) PRIMARY KEY,
    movie_id INT NOT NULL,
    popularity_category NVARCHAR(50) NOT NULL,
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id) 
);


SELECT * FROM movies
GO
SELECT * FROM movies_popularity