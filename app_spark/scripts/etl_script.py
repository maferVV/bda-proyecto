from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("NYC Taxi ETL") \
        .getOrCreate()
    
    # Read from the correct mounted volume
    df = spark.read.parquet("/app/etl_scripts/yellow_tripdata_2025-01.parquet")
    
    # Example transformation
    cleaned_df = df.dropna().filter(df["trip_distance"] > 0)
    
    # Write to Druid-shared volume
    cleaned_df.write.parquet("/opt/shared/processed_taxi_data.parquet")
    
    spark.stop()

if __name__ == "__main__":
    main()

