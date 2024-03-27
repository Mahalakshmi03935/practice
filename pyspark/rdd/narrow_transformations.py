from pyspark.sql import SparkSession

from pyspark import SparkContext

spark = SparkSession.builder.appName("local").getOrCreate()


rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])

mapped_rdd = rdd.map(lambda a : a + 2)
rdd2 = spark.sparkContext.parallelize([1, 2, 3, 4, 5])

filter_rdd2 = rdd.filter(lambda b : b % 2 == 0)

rdd3 = spark.sparkContext.parallelize([1,2,5,6,7])
flat_mapped_rdd3 = rdd3.flatMap(lambda c : c % 2 == 0)

result = mapped_rdd.collect()
result2 = filter_rdd2.collect()
result3 = flat_mapped_rdd3.collect()

print("Result:",result)
print("Result2:",result2)
print("Result3:",result3)
