SELECT ID_Тикета, Статус, SUM(Длительность) Длительность FROM tasketl3b
GROUP BY 1, 2
ORDER BY 1, 2;

SELECT DISTINCT ID_Тикета, Статус, SUM(Длительность) OVER(PARTITION BY ID_Тикета, Статус) Длительность FROM tasketl3b
ORDER BY 1, 2;
