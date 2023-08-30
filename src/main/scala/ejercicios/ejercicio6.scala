package ejercicios

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ejercicio6 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("pruebaSpark")
      .master("local[2]")
      .getOrCreate()

    val customersDf = spark.read.format("avro").load("src/main/resources/retail_db/customers-avro")
    customersDf.show(10, truncate = false)
    customersDf.printSchema()

    val customerNameDf = customersDf.select(col("customer_id"),
      concat(
        substring(col("customer_fname"), 1, 1),
        lit(" "),
        col("customer_lname")).as("customer_name"))

    customerNameDf.show(15)

    val q6Df = customerNameDf.select(
      concat(
        col("customer_id"), lit("\t"),
        col("customer_name")).as("columna_unica"))

    q6Df.show(5, truncate = false)

    q6Df.write
      .mode("overwrite")
      .option("delimiter", "\t")
      .option("compression", "bzip2")
      .text("src/main/dataset/q6/solution")

    val comprobacion6 = spark.read.format("csv")
      .option("delimiter", "\t")
      .load("src/main/dataset/q6/solution")
    comprobacion6.show()


  }

}
