package ejercicios

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ejercicio1 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("LibroSpark")
      .master("local[2]")
      .getOrCreate()

    //añado en el pom la dependencia spark-avro correspondiente a la versión de Spark 3.3.2 y leo los archivos
    val productDf = spark.read.format("avro").load("src/main/resources/retail_db/products_avro")
    productDf.show(10, truncate = false)
    productDf.printSchema()

    //filtro según los criterios del ejercicio (se puede usar también .where)
    val q1Df = productDf
      .filter(
        col("product_price").between(20,23)
          .and(col("product_name").like("Nike%"))
      )
      .orderBy(col("product_price"))
    q1Df.show(30, truncate = false)

    //guardo en el formato especificado
    q1Df.write.format("parquet")
      .mode("overwrite")
      .option("header", "true")
      .option("compression", "gzip")
      .save("src/main/dataset/q1/solution")


  }

}
