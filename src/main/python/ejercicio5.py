from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("pruebaSpark") \
    .master("local[2]") \
    .getOrCreate()

# al cargar el archivo se puede omitir el .format porque parquet es el formato por defecto
product_df = spark.read.load("C:/Users/julia.blanco/Desktop/repositorios/pruebaSpark/src/main/resources/retail_db/orders_parquet")
product_df.show(12, truncate=False)
product_df.printSchema()

# convertir la fecha que está en Unix Timestamp en milisegundos a una columna de tipo Date
todate_df = product_df \
    .withColumn("order_date", (from_unixtime(product_df.order_date / 1000).cast("Timestamp")))

todate_df.show()

q5_df = todate_df \
    .select("order_id", "order_date", "order_status") \
    .filter(
        (product_df.order_status == "COMPLETE") &
        (year(todate_df.order_date) == 2014) &
        ((month(todate_df.order_date) == 1) | (month(todate_df.order_date) == 7))
    ) \
    .withColumn("order_date", date_format(todate_df.order_date, "dd-MM-yyyy"))

q5_df.show(truncate=False)

q5_df.write.format("json") \
    .mode("overwrite") \
    .save("C:/Users/julia.blanco/Desktop/repositorios/pruebaSpark/src/main/dataset/q5/solution")