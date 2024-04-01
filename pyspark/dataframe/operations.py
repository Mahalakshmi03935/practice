from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import Row

spark = SparkSession.builder.appName("LogicalOperations").getOrCreate()


schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True)
])


data = [("Raja", 10),
        ("Maha", 15),
        ("Lakshmi", 20)]

df = spark.createDataFrame(data, schema)
print("Original DataFrame:")
df.show()

# AND 
and_df = df.filter((df["Name"] == "Raja") & (df["Age"] < 20))
print("DataFrame after AND operation:")
and_df.show()


# OR 
or_df = df.filter((df["Name"] == "Raja") | (df["Age"] == 15))
print("DataFrame after OR operation:")
or_df.show()
