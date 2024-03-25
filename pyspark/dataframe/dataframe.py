from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import lit

# Initialize SparkSession
spark = SparkSession.builder.appName("DataFrame").getOrCreate()

# Define the schema for the DataFrame
schema = StructType([
    StructField("name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", IntegerType(), True)
])

# Define the data
data = [
    ('James',   'M', 3000),
    ('Michael', 'M', 4000),
    ('Robert',   'M', 4000),
    ('Maria', 'F', 4000),
    ('Jen', 'F', -1)
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)
df_with_age = df.withColumn("age", lit(30))

# Show DataFrame
df.show()
df_with_age.show()

