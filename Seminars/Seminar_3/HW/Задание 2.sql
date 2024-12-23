/* Задание 2.
В базе данных есть две таблицы: страны и клиенты. 
Одной из потребностей компании является исследование клиентов и стран с точки зрения эффективности продаж, 
поэтому часто выполняются объединения между таблицами: клиенты и страны. 
Что нужно сделать, чтобы ограничить частое объединение этих двух таблиц?
*/

USE spark;

-- 1. Создание таблиц

CREATE TABLE Страны (
UniqueID INT PRIMARY KEY NOT NULL,
country_name VARCHAR(100) NOT NULL
);

CREATE TABLE Клиенты (
UniqueID INT PRIMARY KEY NOT NULL,
customer_name VARCHAR(150) NOT NULL,
country_id INT NOT NULL,
FOREIGN KEY (country_id) REFERENCES Страны(UniqueID)
);

-- 2. Денормализация таблицы Клиенты

-- Добавление нового столбца для названия страны в таблицу Клиенты
ALTER TABLE Клиенты
ADD COLUMN country_name VARCHAR(100);

-- Обновление столбца с названием страны на основе данных из таблицы Страны
UPDATE Клиенты c
JOIN Страны s ON c.country_id = s.UniqueID
SET c.country_name = s.country_name;

-- 3. Триггеры на автоматическое обновление данных

DELIMITER $$

CREATE TRIGGER after_insert_country
AFTER INSERT ON Страны
FOR EACH ROW
BEGIN
UPDATE Клиенты
SET
country_name = NEW.country_name
WHERE country_id = NEW.UniqueID;
END$$

CREATE TRIGGER after_delete_country
AFTER DELETE ON Страны
FOR EACH ROW
BEGIN
UPDATE Клиенты
SET
country_name = NULL
WHERE country_id = OLD.UniqueID;
END$$

DELIMITER ;

-- 4. Заполняем таблицы данными

INSERT INTO Страны (UniqueID, country_name) VALUES
(1, 'Россия'),
(2, 'Беларусь'),
(3, 'Кыргызстан'),
(4, 'Казахстан'),
(5, 'Армения'),
(6, 'Куба'),
(7, 'Венесуэла'),
(8, 'КНДР'),
(9, 'Сирия'),
(10, 'Китай');

INSERT INTO Клиенты (UniqueID, customer_name, country_id) VALUES
(1, 'Иван', 1),
(2, 'Александр', 2),
(3, 'Ырысбек', 3),
(4, 'Улукбек', 4),
(5, 'Арсен', 5),
(6, 'Хуан', 6),
(7, 'Серхио', 7),
(8, 'Чен', 8),
(9, 'Абдулла', 9),
(10, 'Синь', 10);
