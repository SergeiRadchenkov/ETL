/*
chcp 65001 && spark-shell -i "s3.scala" --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
*/
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, collect_list, concat_ws}
import org.apache.spark.sql.{DataFrame, SparkSession}
val t1 = System.currentTimeMillis()
if(1==1){
var df1 = spark.read.format("com.crealytics.spark.excel")
        .option("sheetName", "Sheet1")
        .option("useHeader", "false")
        .option("treatEmptyValuesAsNulls", "false")
        .option("inferSchema", "true").option("addColorColumns", "true")
		.option("usePlainNumberFormat","true")
        .option("startColumn", 0)
        .option("endColumn", 99)
        .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")
        .option("maxRowsInMemory", 20)
        .option("excerptSize", 10)
        .option("header", "true")
        .format("excel")
        .load("s3.xlsx")
		df1.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=23061985")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl3a")
        .mode("overwrite").save()
val q = """
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
(SELECT @PREV1:= ''), (SELECT @PREV2:= '') FROM (SELECT DISTINCT objectid, restime FROM spark.tasketl3a
WHERE fieldname IN ('GNAME2', 'status')) a
LEFT JOIN (SELECT DISTINCT objectid, restime, fieldvalue Статус FROM tasketl3a
WHERE fieldname IN ('status')) a1
ON a.objectid = a1.objectid AND a.restime = a1.restime
LEFT JOIN (SELECT DISTINCT objectid, restime, fieldvalue Группа, 1 Назначение FROM spark.tasketl3a
WHERE fieldname IN ('GNAME2')) a2
ON a.objectid = a1.objectid AND a.restime = a1.restime) b1) b2
"""
spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=23061985")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("query", q)
        .load()
.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=23061985")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl3b")
        .mode("overwrite").save()
}
val s0 = (System.currentTimeMillis() - t1)/1000
val s = s0 % 60
val m = (s0/60) % 60
val h = (s0/60/60) % 24
println("%02d:%02d:%02d".format(h, m, s))
System.exit(0)