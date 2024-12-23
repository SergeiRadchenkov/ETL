USE etl_seminar_4_hw;

-- 1. Создайте таблицу movies с полями movies_type, director, year_of_issue, length_in_minutes, rate.
CREATE TABLE movies (
    movie_id INT AUTO_INCREMENT PRIMARY KEY,
    movies_type VARCHAR(50),
    director VARCHAR(100),
    year_of_issue INT,
    length_in_minutes INT,
    rate DECIMAL(3, 1)
);

-- 2. Сделайте таблицы для горизонтального партицирования по году выпуска (до 1990, 1990 -2000, 2000- 2010, 2010-2020, после 2020).
CREATE TABLE movies_before_1990 LIKE movies;
CREATE TABLE movies_1990_2000 LIKE movies;
CREATE TABLE movies_2000_2010 LIKE movies;
CREATE TABLE movies_2010_2020 LIKE movies;
CREATE TABLE movies_after_2020 LIKE movies;

-- 3. Сделайте таблицы для горизонтального партицирования по длине фильма (до 40 минута, от 40 до 90 минут, от 90 до 130 минут, более 130 минут).
CREATE TABLE movies_short LIKE movies;
CREATE TABLE movies_medium LIKE movies;
CREATE TABLE movies_long LIKE movies;
CREATE TABLE movies_extra_long LIKE movies;

-- 4. Сделайте таблицы для горизонтального партицирования по рейтингу фильма (ниже 5, от 5 до 8, от 8до 10).
CREATE TABLE movies_low_rating LIKE movies;
CREATE TABLE movies_mid_rating LIKE movies;
CREATE TABLE movies_high_rating LIKE movies;

-- 5. Создайте правила добавления данных для каждой таблицы.
-- по году выпуска
DELIMITER //

CREATE TRIGGER movies_insert_year
BEFORE INSERT ON movies
FOR EACH ROW
BEGIN
    IF NEW.year_of_issue < 1990 THEN
        INSERT INTO movies_before_1990 VALUES (NEW.movie_id, NEW.movies_type, NEW.director, NEW.year_of_issue, NEW.length_in_minutes, NEW.rate);
    ELSEIF NEW.year_of_issue BETWEEN 1990 AND 2000 THEN
        INSERT INTO movies_1990_2000 VALUES (NEW.movie_id, NEW.movies_type, NEW.director, NEW.year_of_issue, NEW.length_in_minutes, NEW.rate);
    ELSEIF NEW.year_of_issue BETWEEN 2000 AND 2010 THEN
        INSERT INTO movies_2000_2010 VALUES (NEW.movie_id, NEW.movies_type, NEW.director, NEW.year_of_issue, NEW.length_in_minutes, NEW.rate);
    ELSEIF NEW.year_of_issue BETWEEN 2010 AND 2020 THEN
        INSERT INTO movies_2010_2020 VALUES (NEW.movie_id, NEW.movies_type, NEW.director, NEW.year_of_issue, NEW.length_in_minutes, NEW.rate);
    ELSE
        INSERT INTO movies_after_2020 VALUES (NEW.movie_id, NEW.movies_type, NEW.director, NEW.year_of_issue, NEW.length_in_minutes, NEW.rate);
    END IF;
END //

DELIMITER ;

-- по длине выпуска
DELIMITER //

CREATE TRIGGER movies_insert_lenght
BEFORE INSERT ON movies
FOR EACH ROW
BEGIN
    IF NEW.length_in_minutes < 40 THEN
        INSERT INTO movies_short VALUES (NEW.movie_id, NEW.movies_type, NEW.director, NEW.year_of_issue, NEW.length_in_minutes, NEW.rate);
    ELSEIF NEW.length_in_minutes BETWEEN 40 AND 90 THEN
        INSERT INTO movies_medium VALUES (NEW.movie_id, NEW.movies_type, NEW.director, NEW.year_of_issue, NEW.length_in_minutes, NEW.rate);
    ELSEIF NEW.length_in_minutes BETWEEN 90 AND 130 THEN
        INSERT INTO movies_long VALUES (NEW.movie_id, NEW.movies_type, NEW.director, NEW.year_of_issue, NEW.length_in_minutes, NEW.rate);
    ELSE
        INSERT INTO movies_extra_long VALUES (NEW.movie_id, NEW.movies_type, NEW.director, NEW.year_of_issue, NEW.length_in_minutes, NEW.rate);
    END IF;
END //

DELIMITER ;

-- по рейтингу
DELIMITER //

CREATE TRIGGER movies_insert_raiting
BEFORE INSERT ON movies
FOR EACH ROW
BEGIN
    IF NEW.rate <= 3.3 THEN
        INSERT INTO movies_low_rating VALUES (NEW.movie_id, NEW.movies_type, NEW.director, NEW.year_of_issue, NEW.length_in_minutes, NEW.rate);
    ELSEIF NEW.rate BETWEEN 3.4 AND 6.6 THEN
        INSERT INTO movies_mid_rating VALUES (NEW.movie_id, NEW.movies_type, NEW.director, NEW.year_of_issue, NEW.length_in_minutes, NEW.rate);
    ELSEIF NEW.rate BETWEEN 6.7 AND 10 THEN
        INSERT INTO movies_high_rating VALUES (NEW.movie_id, NEW.movies_type, NEW.director, NEW.year_of_issue, NEW.length_in_minutes, NEW.rate);
    END IF;
END //

DELIMITER ;

-- 6. Добавьте фильмы так, чтобы в каждой таблице было не менее 3 фильмов.
INSERT INTO movies (movies_type, director, year_of_issue, length_in_minutes, rate) VALUES
('Action', 'Director 1', 1985, 120, 7.5),
('Drama', 'Director 2', 1995, 80, 5.5),
('Comedy', 'Director 3', 2005, 100, 8.0),
('Horror', 'Director 4', 2015, 130, 4.5),
('Sci-Fi', 'Director 5', 2021, 150, 9.2),
('Adventure', 'Director 6', 1980, 95, 3.7),
('Fantasy', 'Director 7', 1993, 110, 6.2),
('Thriller', 'Director 8', 2008, 85, 5.8),
('Animation', 'Director 9', 2012, 140, 8.5),
('Documentary', 'Director 10', 2023, 60, 9.0),
('Mystery', 'Director 11', 2023, 55, 12.0),
('Western', 'Director 12', 1978, 130, 11.0),
('Action2', 'Director 4', 1995, 120, 7.5),
('Drama2', 'Director 2', 2005, 80, 5.5),
('Comedy2', 'Director 5', 2015, 100, 8.0),
('Horror2', 'Director 4', 2015, 180, 4.5),
('Sci-Fi2', 'Director 5', 2021, 115, 9.2),
('Adventure2', 'Director 6', 1980, 95, 3.1),
('Fantasy2', 'Director 7', 1993, 110, 2.2),
('Thriller2', 'Director 8', 2008, 85, 1.8),
('Animation2', 'Director 9', 2012, 20, 8.5),
('Documentary2', 'Director 3', 2023, 30, 9.0),
('Mystery2', 'Director 2', 2023, 35, 12.0),
('Western2', 'Director 1', 1978, 130, 11.0);

-- 7. Добавьте пару фильмов с рейтингом выше 10.
INSERT INTO movies (movies_type, director, year_of_issue, length_in_minutes, rate) VALUES
('Action', 'Director 7', 1985, 120, 17.5),
('Drama', 'Director 7', 1995, 80, 15.5),
('Comedy', 'Director 7', 2005, 100, 18.0);

-- 8. Сделайте выбор из всех таблиц, в том числе из основной.
SELECT * FROM movies;
SELECT * FROM movies_before_1990;
SELECT * FROM movies_1990_2000;
SELECT * FROM movies_2000_2010;
SELECT * FROM movies_2010_2020;
SELECT * FROM movies_after_2020;
SELECT * FROM movies_short;
SELECT * FROM movies_medium;
SELECT * FROM movies_long;
SELECT * FROM movies_extra_long;
SELECT * FROM movies_low_rating;
SELECT * FROM movies_mid_rating;
SELECT * FROM movies_high_rating;

-- 9. Сделайте выбор только из основной таблицы.
SELECT * FROM movies;
