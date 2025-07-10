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

# Basic ETL operations
df_clean = (
    df.dropna(how="all")  # drop rows that are entirely empty
      .dropna(subset=["Provincia", "Canton", "Distrito"])  # drop rows missing key locational data
      .withColumnRenamed("Recuento de snb003_id_persona", "count")
      .withColumn("Sexo", trim(col("Sexo")))
      .withColumn("Estado Pobreza", trim(col("Estado Pobreza")))
)

print(f"Number of rows: {df_clean.count()}")
print(f"Number of rows: {df_clean.count()}")
print(f"Number of rows: {df_clean.count()}")
print(f"Number of rows: {df_clean.count()}")

# Save result to directory
output_dir = "/tmp/ssps_modified"
df_clean.coalesce(1).write.csv(output_dir, header=True, mode="overwrite")


# Move the single part file to a flat CSV
final_csv = "/tmp/ssps_modified.csv"

# Find the part file and move it
part_file = None
for f in os.listdir(output_dir):
    if f.startswith("part-") and f.endswith(".csv"):
        part_file = os.path.join(output_dir, f)
        break

if part_file:
    # Remove old final CSV if it exists
    if os.path.exists(final_csv):
        os.remove(final_csv)
    # Move part file to final CSV
    shutil.move(part_file, final_csv)
    # Remove the output directory
    shutil.rmtree(output_dir)

spark.stop()

