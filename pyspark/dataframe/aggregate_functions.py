from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, avg, collect_list, collect_set, countDistinct, count, first, last

spark = SparkSession.builder.appName("data") .getOrCreate()

data = [("Raja", 25),
        ("Maha", 30),
        ("Lakshmi", 35),
        ("Sathya", 28),
        ("Priya", 40),
        ("Ammu", 45)]

df = spark.createDataFrame(data, ["Name", "Age"])
#mean
df.select(mean("Age").alias("Mean Age")).show()
#avg
df.select(avg("Age").alias("Average Age")).show()
#collect-list
df.select(collect_list("Name").alias("Names List")).show()
#collect_set
df.select(collect_set("Name").alias("Names Set")).show()
#count
df.select(countDistinct("Name").alias("Distinct Names Count")).show()
df.select(count("Name").alias("Name Count")).show()
#first
df.select(first("Name").alias("First Name")).show()
#last
df.select(last("Name").alias("Last Name")).show()


spark.stop()
