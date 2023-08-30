package ejercicios

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ejercicio3 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("LibroSpark")
      .master("local[2]")
      .getOrCreate()

    //leer el dataframe para hacernos una idea
    val df = spark.read.text("src/main/resources/retail_db/customers-tab-delimited")
    df.show(10, truncate = false)

    //definir el esquema manualmente
    val schema = StructType(Array(StructField("customer_id", IntegerType, false),
      StructField("customer_fname", StringType, false),
      StructField("customer_lname", StringType, false),
      StructField("customer_email", StringType, false),
      StructField("customer_password", StringType, false),
      StructField("customer_street", StringType, false),
      StructField("customer_city", StringType, false),
      StructField("customer_state", StringType, false),
      StructField("customer_zipcode", StringType, false)))

    //crear el dataframe leyendo los archivos de tipo texto (hay que leerlo como csv para añadir las opciones especificadas)

    val customersDf = spark.read
      .option("delimiter", "\t")
      .schema(schema)
      .csv("src/main/resources/retail_db/customers-tab-delimited")
    customersDf.show(20, truncate = false)

    //Se puede hacer infiriendo esquema y añadiendo luego los nombres de las columnas
    /*
    val customers1Df = spark.read
      .option("delimiter", "\t")
      .option("inferSchema", "true")
      .csv("src/main/resources/retail_db/customers-tab-delimited")
    customers1Df.show(20, truncate = false)
    customers1Df.printSchema()

    val customersDf = customers1Df.toDF("customer_id", "customer_fname", "customer_lname", "customer_email", "customer_password", "customer_street", "customer_city", "customer_state", "customer_zipcode")
    customersDf.show()
    */

  //Get total number of customers in each state whose first name starts with A and total customer count is greater than 50

    val q3Df = customersDf
      .where(col("customer_fname").like("A%"))
      .groupBy(col("customer_state").as("state"))
      .agg(count("customer_id").as("count"))
      .where(col("count") > 50)
    q3Df.show(30, truncate = false)

    q3Df.write.format("parquet")
      .mode("overwrite")
      .option("compression", "gzip")
      .save("src/main/dataset/q3/solution")

    val comprobacion = spark.read.format("parquet").load("src/main/dataset/q3/solution")
    comprobacion.show()

  }

}
