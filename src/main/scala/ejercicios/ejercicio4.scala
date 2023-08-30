package ejercicios

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ejercicio4 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("LibroSpark")
      .master("local[2]")
      .getOrCreate()

    //leer el dataframe para hacernos una idea
    val df = spark.read.text("src/main/resources/retail_db/categories")
    df.show(10, truncate = false)
    //definir el esquema manualmente
    val schema = StructType(Array(StructField("category_id", IntegerType, false),
      StructField("category_department_id", IntegerType, false),
      StructField("category_name", StringType, false)))

    //crear el dataframe leyendo los archivos de tipo texto (hay que leerlo como csv para añadir las opciones especificadas)
    val categories4Df = spark.read
      .option("delimiter", ",")
      .schema(schema)
      .csv("src/main/resources/retail_db/categories")

    categories4Df.show(30, truncate = false)

    val soccerDf = categories4Df
      .where(col("category_name").equalTo("Soccer"))

    soccerDf.show()
  //convertir el dataframe a una única columna con pipe delimiter, para poder guardarlo en formato texto.
    val q4Df = soccerDf.select(
      concat(
        col("category_id"), lit('|'),
        col("category_department_id"), lit('|'),
        col("category_name"))
        .as("columna_unica"))

    q4Df.show(truncate = false)

    //guardarlo en formato texto

      q4Df.write
      .mode("overwrite")
      .option("delimiter", "|")
      .text("src/main/dataset/q4/solution")

    val comprobacion4 = spark.read.format("csv")
      .option("delimiter", "|")
      .load("src/main/dataset/q4/solution")
    comprobacion4.show()


  }

}
