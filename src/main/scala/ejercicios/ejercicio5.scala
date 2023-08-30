package ejercicios

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ejercicio5 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("LibroSpark")
      .master("local[2]")
      .getOrCreate()

    //al cargar el archivo se puede omitir el .format porque parquet es el formato por defecto
    val productDf = spark.read.load("src/main/resources/retail_db/orders_parquet")
    productDf.show(12, truncate = false)
    productDf.printSchema()

    //convertir la fecha que está en Unix Timestamp en milisegundos a una columna de tipo Date


    val toDateDf = productDf
      .withColumn("order_date", from_unixtime(col("order_date") / 1000).cast("Timestamp"))

    toDateDf.show()




    val q5Df = toDateDf
      .select("order_id", "order_date", "order_status")
      .where(
        col("order_status").equalTo("COMPLETE")
          .and(year(col("order_date")).equalTo(2014)
            .and(
              month(col("order_date")).equalTo(1)
                .or(month(col("order_date")).equalTo(7))
            )
          )
      )
      .withColumn("order_date", date_format(col("order_date"), "dd-MM-yyyy"))

    q5Df.show(truncate = false)

    q5Df.write.format("json")
      .mode("overwrite")
      .save("src/main/dataset/q5/solution")


  }

}
