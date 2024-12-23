/*
chcp 65001 && spark-shell -i fifa_s2.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
*/
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

val spark = SparkSession.builder
  .appName("FIFA Data Analysis")
  .config("spark.master", "local")
  .getOrCreate()

var df1 = spark.read.option("delimiter",",")
        .option("header", "true")
        .csv("fifa_s2.csv")

// Задание 1: Очистка данных
val columnsToDrop = Seq("Joined", "Contract Valid Until")
var cleanedDf = df1.drop(columnsToDrop: _*)
cleanedDf = cleanedDf.na.drop()

// Задание 2: Удаление полных дубликатов
cleanedDf = cleanedDf.withColumn("Club_lowercase", lower(col("Club")))
val uniqueDf = cleanedDf.dropDuplicates()

// Задание 3: Добавление колонки с разбиением возраста по группам
val ageGroupsDf = uniqueDf.withColumn("AgeGroup", 
  when(col("Age") < 20, "до 20")
    .when(col("Age").between(20, 30), "от 20 до 30")
    .when(col("Age").between(30, 36), "от 30 до 36")
    .otherwise("старше 36")
)

val ageGroupCounts = ageGroupsDf.groupBy("AgeGroup").count()
ageGroupCounts.show()

df1.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=23061985")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "FIFA_Data")
        .mode("overwrite").save()
		df1.show()

spark.stop()
System.exit(0)