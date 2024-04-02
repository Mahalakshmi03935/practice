from pyspark.sql import SparkSession


spark = SparkSession.builder .appName("data") .getOrCreate()

data1 = [("Maha", 25),
         ("Raja", 30),
         ("Lakshmi", 35)]

data2 = [("Raja", 30),
         ("Ammu", 40),
         ("Raji", 45)]

df1 = spark.createDataFrame(data1, ["Name", "Age"])
df2 = spark.createDataFrame(data2, ["Name", "Age"])

union_df = df1.union(df2)
print("Union (Removing Duplicates):")
union_df.show()

union_all_df = df1.unionAll(df2)
print("UnionAll (Retaining Duplicates):")
union_all_df.show()


union_by_name_df = df1.unionByName(df2)
print("UnionByName (Merging by Name):")
union_by_name_df.show()


spark.stop()
