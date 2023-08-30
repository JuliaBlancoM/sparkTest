package ejercicios

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ejercicio3 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("pruebaSpark")
      .master("local[2]")
      .enableHiveSupport()
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

  //Get total number of customers in each state whose first name starts with A and total customer count is greater than 50

    val q3Df = customersDf
      .where(col("customer_fname").like("A%"))
      .groupBy(col("customer_state").as("state"))
      .agg(count("customer_id").as("count"))
      .where(col("count") > 50)
    q3Df.show(30, truncate = false)

    q3Df.write.format("parquet")
      .mode("overwrite")
      .option("header", "true")
      .option("compression", "gzip")
      .save("src/main/dataset/q3/solution")

    val comprobacion = spark.read.format("parquet").load("src/main/dataset/q3/solution")
    comprobacion.show()

    //Guardar el DataFrame en Hive Metastore (no lo dejo comentado porque creo que funciona):
    spark.sql("CREATE DATABASE IF NOT EXISTS customers")
    q3Df.write.mode("overwrite").saveAsTable("customers.q3table")
    //Esto crea una managed table. El nombre de la tabla debe ser "nombrebasededatos.nombretabla (por eso creo la base
    //de datos)
    //Al estar en local, se crea una metastore de Hive que se llama metastore_db y una warehouse location llamada spark-warehouse
    // dentro del directorio del repositorio

  }

}
