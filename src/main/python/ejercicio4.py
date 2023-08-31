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
categories4_df = spark.read \
    .option("delimiter", ",") \
    .schema(schema) \
    .csv("C:/Users/julia.blanco/Desktop/repositorios/pruebaSpark/src/main/resources/retail_db/categories")
categories4_df.show(30, truncate=False)

soccer_df = categories4_df.filter(categories4_df.category_name == "Soccer")
soccer_df.show()

# convertir el dataframe a una única columna con pipe delimiter, para poder guardarlo en formato texto.
q4_df = soccer_df.select(
    concat(
        categories4_df.category_id, lit("|"),
        categories4_df.category_department_id, lit("|"),
        categories4_df.category_name
    ).alias("columna_unica")
)
q4_df.show(truncate=False)

# guardarlo en formato texto
q4_df.write \
    .mode("overwrite") \
    .option("delimiter", "|") \
    .text("C:/Users/julia.blanco/Desktop/repositorios/pruebaSpark/src/main/dataset/q4/solution")

comprobacion4 = spark.read.format("csv") \
    .option("delimiter", "|") \
    .load("C:/Users/julia.blanco/Desktop/repositorios/pruebaSpark/src/main/dataset/q4/solution")
comprobacion4.show()