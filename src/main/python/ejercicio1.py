from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("pruebaSpark") \
    .master("local[2]") \
    .getOrCreate()

# leo los archivos
product_df = spark.read.format("avro").load("C:/Users/julia.blanco/Desktop/repositorios/pruebaSpark/src/main/resources/retail_db/products_avro")
product_df.show(10, truncate=False)
product_df.printSchema()

# filtro según los criterios del ejercicio (se puede usar también .where)
q1_df = product_df.filter(
    (product_df.product_price.between(20,23)) &
    (product_df.product_name.like("Nike%"))
).orderBy(product_df.product_price)
q1_df.show(30, truncate=False)

# guardo en el formato especificado
q1_df.write.format("parquet") \
    .mode("overwrite") \
    .option("header", "true") \
    .option("compression", "gzip") \
    .save("C:/Users/julia.blanco/Desktop/repositorios/pruebaSpark/src/main/dataset/q1/solution")