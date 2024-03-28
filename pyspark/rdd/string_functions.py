from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Data").getOrCreate()
data = (   "raja" ,"maha", "lakshmi"  )
rdd = spark.sparkContext.parallelize(data)
val=rdd.collect()
print(rdd.collect())
#string functions
#uppercase
uppercase = rdd.map(lambda x: x.upper())
print(uppercase.collect())
upper = rdd.map(lambda a :a.title())
print(upper.collect())
#lowercase
lower = rdd.map(lambda b : b.lower())
print("The lower case is :" , lower.collect())
#split
splitted = rdd.map(lambda c : c.split())
print(splitted.collect())
#strip
stripped_rdd = rdd.map(lambda d : d.strip())
print(stripped_rdd.collect())
#replace
replaced_rdd = rdd.map(lambda f: f.replace("raja","Balraj"))
print(replaced_rdd.collect())



