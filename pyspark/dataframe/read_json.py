from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("data").getOrCreate()
df = spark.read.option("header","true").json('C:/Users/RajaMahalakshmiB/Desktop/practice/practice/pyspark/resource_file/sample.json')
df.show()

