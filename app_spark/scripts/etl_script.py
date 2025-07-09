from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ETL Script") \
    .master("spark://spark:7077") \
    .getOrCreate()

# Read the Parquet file
input_path = "/app/data/yellow_tripdata_2025-01.parquet"
df = spark.read.parquet(input_path)

# Example transformation: Filter for rides with a passenger count greater than 1
transformed_df = df.filter(df.passenger_count > 1)

# check if data can be written
if transformed_df.count() > 0:
    transformed_df.write.parquet(output_path)
else:
    print("No data to write.")

# Write the transformed data to the Druid-shared volume
output_path = "/app/druid_shared/yellow_tripdata_transformed.parquet"
transformed_df.write.parquet(output_path)

# Stop the Spark session
spark.stop()
