package ejercicios

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ejercicio4 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("pruebaSpark")
      .master("local[2]")
      .getOrCreate()

    //leer el dataframe para hacernos una idea
    val df = spark.read.text("src/main/resources/retail_db/categories")
    df.show(10, truncate = false)

    //crear el dataframe leyendo los archivos de tipo texto (hay que leerlo como csv para añadir las opciones especificadas)
    val inicialDf = spark.read
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .csv("src/main/resources/retail_db/categories")

    inicialDf.show(30, truncate = false)

    val categories4Df = inicialDf.toDF("category_id", "category_department_id", "category_name")

    val q4Df = categories4Df
      .where(col("category_name").equalTo("Soccer"))

    q4Df.show()

    //guardarlo en formato texto

      q4Df.write
      .mode("overwrite")
      .option("delimiter", "|")
      .csv("src/main/dataset/q4/solution")

    val comprobacion4 = spark.read.format("csv")
      .option("delimiter", "|")
      .load("src/main/dataset/q4/solution")
    comprobacion4.show()


  }

}
