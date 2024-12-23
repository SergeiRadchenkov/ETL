import pyspark, time, platform, sys, os
from datetime import datetime
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
import pandas as pd
import matplotlib.pyplot as plt 
from sqlalchemy import inspect, create_engine, text
import warnings, matplotlib
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as sum1
warnings.filterwarnings("ignore")
t0 = time.time()
con = create_engine("mysql://root:23061985@localhost/spark")
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark=SparkSession.builder.appName("Hi").getOrCreate()

with con.connect() as connection:
        connection.execute(text("""drop table if exists spark.`tasketl4b`"""))
        connection.execute(text("""CREATE TABLE if not exists spark.`tasketl4b` (
	            `№` INT(10) NULL DEFAULT NULL,
	            `Месяц` DATE NULL DEFAULT NULL,
	            `Сумма платежа` FLOAT NULL DEFAULT NULL,
	            `Платеж по основному долгу` FLOAT NULL DEFAULT NULL,
	            `Платеж по процентам` FLOAT NULL DEFAULT NULL,
	            `Остаток долга` FLOAT NULL DEFAULT NULL,
	            `проценты` FLOAT NULL DEFAULT NULL,
	            `долг` FLOAT NULL DEFAULT NULL
)
COLLATE='utf8mb4_0900_ai_ci'
ENGINE=InnoDB"""))

w = Window.partitionBy(lit(1)).orderBy("№").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df1 = spark.read.format("com.crealytics.spark.excel")\
        .option("sheetName", "Sheet1")\
        .option("useHeader", "false")\
        .option("treatEmptyValuesAsNulls", "false")\
        .option("inferSchema", "true").option("addColorColumns", "true")\
	    .option("usePlainNumberFormat","true")\
        .option("startColumn", 0)\
        .option("endColumn", 99)\
        .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")\
        .option("maxRowsInMemory", 20)\
        .option("excerptSize", 10)\
        .option("header", "true")\
        .format("excel")\
        .load("s4_2.xlsx").limit(1000)\
        .withColumn("проценты", sum1(col("Платеж по процентам")).over(w))\
        .withColumn("долг", sum1(col("Платеж по основному долгу")).over(w))

df1.write.format("jdbc").option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=23061985")\
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl4b")\
        .mode("append").save()

df2 = df1.toPandas()
# Get current axis
ax = plt.gca()
ax.ticklabel_format(style='plain')
# bar plot
df2.plot(kind='line',
        x='№',
        y='долг',
        color='green', ax=ax)
df2.plot(kind='line',
        x='№',
        y='проценты',
        color='red', ax=ax)
# set the title
plt.title('Выплаты')
plt.grid ( True )
ax.set(xlabel=None)
# show the plot
plt.show()

spark.stop()
t1 = time.time()
print('finished', time.strftime('%H:%M:%S', time.gmtime(round(t1-t0))))
