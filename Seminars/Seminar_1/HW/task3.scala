/*
chcp 65001 && spark-shell -i "task3.scala" --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
:load task3.scala
*/
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions._
import com.crealytics.spark.excel._

object task3 {
  def main(args: Array[String]): Unit = {
    // Создание SparkSession
    val spark = SparkSession.builder()
      .appName("Normalization Example")
      .config("spark.master", "local")
      .getOrCreate()

    // Чтение данных из Excel файла
    val df = spark.read
      .format("com.crealytics.spark.excel")
      .option("header", "true") // Использовать первую строку как заголовки
      .option("inferSchema", "true") // Автоматически определять типы данных
      .load("task3.xlsx") // Укажите путь к вашему Excel файлу

    // Создание таблицы "Сотрудники"
    val employeesDF = df.select("Employee_ID", "Name", "City_code", "Home_city").distinct()
    
    // Создание таблицы "Работы"
    val jobsDF = df.select("Job_Code", "Job").distinct()
    
    // Создание таблицы "Детали работы"
    val employeeJobsDF = df.select("Employee_ID", "Job_Code").distinct()

    // Настройка соединения с базой данных
    val jdbcUrl = "jdbc:mysql://localhost:3306/etl_seminar_1_hw_task_3"
    val connectionProperties = new java.util.Properties()
    connectionProperties.setProperty("user", "root")
    connectionProperties.setProperty("password", "23061985")

    // Запись нормализованных таблиц в SQL базу данных
    employeesDF.write.mode(SaveMode.Overwrite).jdbc(jdbcUrl, "Employees", connectionProperties)
    jobsDF.write.mode(SaveMode.Overwrite).jdbc(jdbcUrl, "Jobs", connectionProperties)
    employeeJobsDF.write.mode(SaveMode.Overwrite).jdbc(jdbcUrl, "Employee_Jobs", connectionProperties)

    // Остановка SparkSession
    spark.stop()
  }
}
