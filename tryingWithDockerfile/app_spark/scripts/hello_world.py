from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("HelloWorld") \
    .master("spark://spark-master:7077") \
    .getOrCreate()
# Simple Spark job
data = ["Hello", "World!", "This", "is", "Spark!"]
rdd = spark.sparkContext.parallelize(data)
result = rdd.collect()
for word in result:
    print(word)
spark.stop()
