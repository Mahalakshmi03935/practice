from pyspark.sql import SparkSession
from pyspark.sql import column
spark = SparkSession.builder.appName('data').getOrCreate()
df = spark.read.option('header','true').csv('name.csv',inferSchema=True)
df.show()
df.printSchema()
selected_df = df.select("Name")
selected_df.show()
filtered_df = df.filter(lambda column2 : column2 < 0)
filtered_df.show()
