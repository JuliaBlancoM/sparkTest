from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("pruebaSpark") \
    .master("local[2]") \
    .getOrCreate()

# leer el dataframe para hacernos una idea
df = spark.read.text("C:/Users/julia.blanco/Desktop/repositorios/pruebaSpark/src/main/resources/retail_db/customers-tab-delimited")
df.show(10, truncate=False)

# definir el esquema manualmente
schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("customer_fname", StringType(), False),
    StructField("customer_lname", StringType(), False),
    StructField("customer_email", StringType(), False),
    StructField("customer_password", StringType(), False),
    StructField("customer_street", StringType(), False),
    StructField("customer_city", StringType(), False),
    StructField("customer_state", StringType(), False),
    StructField("customer_zipcode", StringType(), False)
])

# crear el dataframe leyendo los archivos de tipo texto (hay que leerlo como csv para añadir las opciones especificadas)
customers_df = spark.read \
    .option("delimiter", "\t") \
    .schema(schema) \
    .csv("C:/Users/julia.blanco/Desktop/repositorios/pruebaSpark/src/main/resources/retail_db/customers-tab-delimited")
customers_df.show(20, truncate=False)

# Get total number of customers in each state whose first name starts with A and total customer count is greater than 50
q3_df = customers_df \
    .filter(customers_df.customer_fname.like("A%")) \
    .groupBy(customers_df.customer_state.alias("state")) \
    .agg(count(customers_df.customer_id).alias("count")) \
    .filter(col("count") > 50)
q3_df.show(30, truncate=False)

q3_df.write.format("parquet") \
    .mode("overwrite") \
    .option("header", "true") \
    .option("compression", "gzip") \
    .save("C:/Users/julia.blanco/Desktop/repositorios/pruebaSpark/src/main/dataset/q3/solution")

comprobacion = spark.read.format("parquet").load("C:/Users/julia.blanco/Desktop/repositorios/pruebaSpark/src/main/dataset/q3/solution")
comprobacion.show()

# Guardar el DataFrame en Hive Metastore (no lo dejo comentado porque creo que funciona):
#spark.sql("CREATE DATABASE IF NOT EXISTS customers")
#q3Df.write.mode("overwrite").saveAsTable("customers.q3table")
# Esto crea una managed table. El nombre de la tabla debe ser "nombrebasededatos.nombretabla" (por eso creo la base de datos)
# Al estar en local, se crea una metastore de Hive que se llama metastore_db y una warehouse location llamada spark-warehouse
# dentro del directorio del repositorio