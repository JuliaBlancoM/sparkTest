package ejercicios

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ejercicio2 {

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
    val categoriesDf = spark.read
      .option("delimiter", ",")
      .schema(schema)
      .csv("src/main/resources/retail_db/categories")

    categoriesDf.show(30, truncate = false)

    val q2Df = categoriesDf
      .select("category_id", "category_name")
      .orderBy(col("category_id").desc)

    q2Df.show(25, truncate = false)

    //guardarlo como un solo archivo (se puede usar coalesce(1) o repartition(1) pero coalesce usa menos recursos)
    q2Df.coalesce(1)
      .write
      .mode("overwrite")
      .option("delimiter", ":")
      .csv("src/main/dataset/q2/solution")





  }

}
