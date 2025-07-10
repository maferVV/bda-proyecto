from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ETL").getOrCreate()

# Extract
df = spark.read.parquet("/app/data/yellow_tripdata_2025-01.parquet")

# Transform
df_clean = df.dropna().dropDuplicates()

# Load
df_clean.write.parquet("/opt/spark-app/output", mode="overwrite")

spark.stop()
