/* Задание 3.
Вернемся к первому примеру. 
Предположим, компания хочет регулярно извлекать данные о продажах, например, 
о кампаниях или рекламодателях с полными именами. 
Как мы можем решить проблему постоянной необходимости
объединения таблиц?
*/

USE spark;

-- 1. Обновим структуру таблицы Продажи

-- Добавление новых столбцов
ALTER TABLE Продажи
ADD COLUMN advertiser_name VARCHAR(100),
ADD COLUMN campaign_name VARCHAR(150);

-- Обновление значения новых столбцов
UPDATE Продажи p
JOIN Рекламодатели r ON p.advertise_id = r.UniqueID
JOIN Кампания c ON p.campaign_id  = c.UniqueID
SET p.advertiser_name = r.Name,
p.campaign_name = c.campaign_name;

-- 2. Триггеры на автоматическое обновление данных

DELIMITER $$

CREATE TRIGGER after_update_advertiser
AFTER INSERT ON Рекламодатели
FOR EACH ROW
BEGIN
UPDATE Продажи
SET
advertiser_name = NEW.Name
WHERE advertise_id = NEW.UniqueID;
END$$

CREATE TRIGGER after_update_campaign
AFTER DELETE ON Кампания
FOR EACH ROW
BEGIN
UPDATE Продажи
SET
campaign_name = NEW.campaign_name
WHERE campaign_id  = OLD.UniqueID;
END$$

DELIMITER ;