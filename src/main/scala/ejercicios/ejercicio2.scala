package ejercicios

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ejercicio2 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("pruebaSpark")
      .master("local[2]")
      .getOrCreate()

    //leer el dataframe para hacernos una idea
    val df = spark.read.text("src/main/resources/retail_db/categories")
    df.show(10, truncate = false)

    //crear el dataframe leyendo los archivos de tipo texto.
    val primerDf = spark.read
      .option("delimiter", ",")
      .option("infereSchema", "true")
      .csv("src/main/resources/retail_db/categories")

    primerDf.show(30, truncate = false)
    primerDf.printSchema()

    val categoriesDf = primerDf.toDF("category_id", "category_department_id", "category_name")
    categoriesDf.show(13, truncate = false)

    val q2Df = categoriesDf
      .select("category_id", "category_name")
      .orderBy(col("category_id").desc)

    q2Df.show(25, truncate = false)

    //guardarlo como un solo archivo (se puede usar coalesce(1) o repartition(1) pero coalesce usa menos recursos)
    q2Df.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .option("delimiter", ":")
      .csv("src/main/dataset/q2/solution")




  }

}
