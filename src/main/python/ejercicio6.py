from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("pruebaSpark") \
    .master("local[2]") \
    .getOrCreate()

customers_df = spark.read.format("avro").load("C:/Users/julia.blanco/Desktop/repositorios/pruebaSpark/src/main/resources/retail_db/customers-avro")
customers_df.show(10, truncate=False)
customers_df.printSchema()

customer_name_df = customers_df.select(
    customers_df.customer_id,
    concat(
        substring(customers_df.customer_fname, 1, 1),
        lit(" "),
        customers_df.customer_lname
    ).alias("customer_name")
)

customer_name_df.show(15)

q6_df = customer_name_df.select(
    concat(
        customer_name_df.customer_id,
        lit("\t"),
        customer_name_df.customer_name
    ).alias("columna_unica")
)

q6_df.show(5, truncate=False)

q6_df.write \
    .mode("overwrite") \
    .option("delimiter", "\t") \
    .option("compression", "bzip2") \
    .text("C:/Users/julia.blanco/Desktop/repositorios/pruebaSpark/src/main/dataset/q6/solution")

comprobacion6 = spark.read.format("csv") \
    .option("delimiter", "\t") \
    .load("C:/Users/julia.blanco/Desktop/repositorios/pruebaSpark/src/main/dataset/q6/solution")
comprobacion6.show()