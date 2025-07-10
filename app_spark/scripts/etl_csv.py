from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
import os
import shutil
import logging

# Set logging level
logging.getLogger("org").setLevel(logging.ERROR)
logging.getLogger("akka").setLevel(logging.ERROR)

# Start Spark session
spark = SparkSession.builder \
    .appName("ETL_ssps") \
    .master("spark://spark:7077") \
    .getOrCreate()

# Load CSV
df = spark.read.csv("/app/data/ssps.csv", header=True, encoding="utf-8", inferSchema=True)

print(f"Number of rows before ETL operations: {df.count()}")


# Basic ETL operations
df_clean = (
    df.dropna(how="all")  # drop rows that are entirely empty
      .dropna(subset=["Provincia", "Canton", "Distrito"])  # drop rows missing key locational data
      .withColumnRenamed("Recuento de snb003_id_persona", "count")
      .withColumn("Sexo", trim(col("Sexo")))
      .withColumn("Estado Pobreza", trim(col("Estado Pobreza")))
)

print(f"Number of rows after ETL operations: {df_clean.count()}")

# Save result to directory
#output_dir = "/tmp/ssps_modified"
output_dir = "/app/data/ssps_modified"
df_clean.coalesce(1).write.csv(output_dir, header=True, mode="overwrite")

spark.stop()

