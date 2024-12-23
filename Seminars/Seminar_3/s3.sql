USE spark;

SELECT ID_Тикета, FROM_UNIXTIME(Status_time) Status_time, 
(LEAD(Status_time) OVER(PARTITION BY ID_Тикета ORDER BY Status_time) - Status_time) / 3600 Длительность, 
CASE WHEN Статус IS NULL THEN @PREV1
ELSE @PREV1:= Статус END
Статус,
CASE WHEN Группа IS NULL THEN @PREV2
ELSE @PREV2:= Группа END
Группа, Назначение FROM 
(SELECT ID_Тикета, Status_time, Статус, IF (ROW_NUMBER() OVER(PARTITION BY ID_Тикета ORDER BY Status_time) = 1 AND Назначение IS NULL, '', Группа) Группа, Назначение FROM 
(SELECT DISTINCT a.objectid ID_Тикета, a.restime Status_time, Статус, Группа, Назначение, 
(SELECT @PREV1:= ''), (SELECT @PREV2:= '') FROM (SELECT DISTINCT objectid, restime FROM tasketl3a
WHERE fieldname IN ('GNAME2', 'status')) a
LEFT JOIN (SELECT DISTINCT objectid, restime, fieldvalue Статус FROM tasketl3a
WHERE fieldname IN ('status')) a1
ON a.objectid = a1.objectid AND a.restime = a1.restime
LEFT JOIN (SELECT DISTINCT objectid, restime, fieldvalue Группа, 1 Назначение FROM tasketl3a
WHERE fieldname IN ('GNAME2')) a2
ON a.objectid = a1.objectid AND a.restime = a1.restime) b1) b2
