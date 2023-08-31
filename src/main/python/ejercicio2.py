from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("pruebaSpark") \
    .master("local[2]") \
    .getOrCreate()


# leer el dataframe para hacernos una idea
df = spark.read.text("C:/Users/julia.blanco/Desktop/repositorios/pruebaSpark/src/main/resources/retail_db/categories")
df.show(10, truncate=False)

# definir el esquema manualmente
schema = StructType([
    StructField("category_id", IntegerType(), False),
    StructField("category_department_id", IntegerType(), False),
    StructField("category_name", StringType(), False)
])

# crear el dataframe leyendo los archivos de tipo texto (hay que leerlo como csv para añadir las opciones especificadas)
categories_df = spark.read \
    .option("delimiter", ",") \
    .schema(schema) \
    .csv("C:/Users/julia.blanco/Desktop/repositorios/pruebaSpark/src/main/resources/retail_db/categories")

categories_df.show(30, truncate=False)

q2_df = categories_df \
    .select("category_id", "category_name") \
    .orderBy(categories_df.category_id.desc())

q2_df.show(25, truncate=False)

# guardarlo como un solo archivo (se puede usar coalesce(1) o repartition(1) pero coalesce usa menos recursos)
q2_df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .option("delimiter", ":") \
    .csv("C:/Users/julia.blanco/Desktop/repositorios/pruebaSpark/src/main/dataset/q2/solution")