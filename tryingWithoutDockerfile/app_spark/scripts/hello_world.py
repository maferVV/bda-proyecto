from pyspark.sql import SparkSession

# Update the master URL to use the correct service name
spark = SparkSession.builder \
    .appName("HelloWorld") \
    .master("spark://spark:7077") \
    .getOrCreate()

# Simple Spark job
data = ["Hello", "World!", "This", "is", "Spark!"]
rdd = spark.sparkContext.parallelize(data)
result = rdd.collect()
for word in result:
    print(word)

spark.stop()

