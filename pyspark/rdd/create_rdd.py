from pyspark.sql import SparkSession
# Create a SparkSession
spark = SparkSession.builder.appName("Simple RDD Creation") .getOrCreate()
# Create an RDD from a list
data = [1, 2, 3, 4, 5]
rdd = spark.sparkContext.parallelize(data)
# Print RDD
print(rdd)
# Stop SparkSession
spark.stop()

#Transformations on existing RDDs
rdd = sc.parallelize(range(1, 6))
transformed_rdd = rdd.map(lambda x: x * 2)

#Creating RDD from key-value pairs
kv_data = [("a", 1), ("b", 2), ("c", 3)]
kv_rdd = sc.parallelize(kv_data)

#Using range() function
rdd = sc.range(1, 6)


