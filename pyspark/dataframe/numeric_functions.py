from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, min, max, round, abs

# Create a SparkSession
spark = SparkSession.builder.appName("NumericFunctions").getOrCreate()

# Read your DataFrame, assuming it's named df

# Assuming df contains a numeric column named "numeric_column"

# Sum()
sum_result = df.select(sum("numeric_column")).collect()[0][0]

# Avg()
avg_result = df.select(avg("numeric_column")).collect()[0][0]

# Min()
min_result = df.select(min("numeric_column")).collect()[0][0]

# Max()
max_result = df.select(max("numeric_column")).collect()[0][0]

# Round() (round to 2 decimal places)
rounded_df = df.withColumn("rounded_numeric_column", round("numeric_column", 2))

# Abs()
abs_df = df.withColumn("abs_numeric_column", abs("numeric_column"))

# Show results
print("Sum:", sum_result)
print("Avg:", avg_result)
print("Min:", min_result)
print("Max:", max_result)
rounded_df.show()
abs_df.show()
