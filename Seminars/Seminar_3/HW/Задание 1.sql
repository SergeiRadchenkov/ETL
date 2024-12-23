-- Задание 1. Денормализуйте таблицу так, чтобы не нужно было для каждого рекламодателя постоянно подсчитывать количество кампаний и продаж.

USE spark;

-- 1. Создание таблиц

CREATE TABLE Рекламодатели (
UniqueID INT PRIMARY KEY NOT NULL,
Name VARCHAR(100) NOT NULL
);

CREATE TABLE Кампания (
UniqueID INT PRIMARY KEY NOT NULL,
campaign_name VARCHAR(150) NOT NULL
);

CREATE TABLE Продажи (
UniqueID INT PRIMARY KEY NOT NULL,
advertise_id INT NOT NULL,
campaign_id INT NOT NULL,
FOREIGN KEY (advertise_id) REFERENCES Рекламодатели(UniqueID),
FOREIGN KEY (campaign_id) REFERENCES Кампания(UniqueID)
);

-- 2. Денормализация таблицы Рекламодатели

ALTER TABLE Рекламодатели
ADD COLUMN total_campaigns INT DEFAULT 0,
ADD COLUMN total_sales INT DEFAULT 0;

UPDATE Рекламодатели r
SET
total_campaigns = (SELECT COUNT(DISTINCT p.campaign_id) FROM Продажи p WHERE p.advertise_id = r.UniqueID),
total_sales = (SELECT COUNT(*) FROM Продажи p WHERE p.advertise_id = r.UniqueID);

-- 3. Триггеры на автоматическое обновление данных

DELIMITER $$

CREATE TRIGGER after_insert_sales
AFTER INSERT ON Продажи
FOR EACH ROW
BEGIN
UPDATE Рекламодатели
SET
total_campaigns = (SELECT COUNT(DISTINCT p.campaign_id) FROM Продажи WHERE advertise_id = NEW.advertise_id),
total_sales = (SELECT COUNT(*) FROM Продажи WHERE advertise_id = NEW.advertise_id)
WHERE UniqueID = NEW.advertise_id;
END$$

CREATE TRIGGER after_delete_sales
AFTER DELETE ON Продажи
FOR EACH ROW
BEGIN
UPDATE Рекламодатели
SET
total_campaigns = (SELECT COUNT(DISTINCT p.campaign_id) FROM Продажи WHERE advertise_id = OLD.advertise_id),
total_sales = (SELECT COUNT(*) FROM Продажи WHERE advertise_id = OLD.advertise_id)
WHERE UniqueID = OLD.advertise_id;
END$$

DELIMITER ;

-- 4. Заполняем таблицы данными

INSERT INTO Рекламодатели (UniqueID, Name) VALUES
(1, 'Advertiser A'),
(2, 'Advertiser B'),
(3, 'Advertiser C'),
(4, 'Advertiser D'),
(5, 'Advertiser E'),
(6, 'Advertiser F'),
(7, 'Advertiser G'),
(8, 'Advertiser H'),
(9, 'Advertiser I'),
(10, 'Advertiser J');

INSERT INTO Кампания (UniqueID, campaign_name) VALUES
(1, 'Campaign A'),
(2, 'Campaign B'),
(3, 'Campaign С'),
(4, 'Campaign D'),
(5, 'Campaign E'),
(6, 'Campaign F'),
(7, 'Campaign G'),
(8, 'Campaign H'),
(9, 'Campaign I'),
(10, 'Campaign J');

INSERT INTO Продажи (UniqueID, advertise_id, campaign_id) VALUES
(1, 1, 1),
(2, 2, 2),
(3, 3, 3),
(4, 4, 4),
(5, 5, 5),
(6, 1, 6),
(7, 2, 7),
(8, 3, 8),
(9, 4, 9),
(10, 5, 10);
