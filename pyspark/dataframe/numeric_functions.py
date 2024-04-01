from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, min, max, round, abs

spark = SparkSession.builder.appName("NumericFunctions").getOrCreate()



# Sum()
sum_result = df.select(sum("numeric_column")).collect()[0][0]
print("Sum:", sum_result)

# Avg()
avg_result = df.select(avg("numeric_column")).collect()[0][0]
print("Avg:", avg_result)

# Min()
min_result = df.select(min("numeric_column")).collect()[0][0]
print("Min:", min_result)

# Max()
max_result = df.select(max("numeric_column")).collect()[0][0]
print("Max:", max_result)

# Round() (round to 2 decimal places)
rounded_df = df.withColumn("rounded_numeric_column", round("numeric_column", 2))
rounded_df.show()

# Abs()
abs_df = df.withColumn("abs_numeric_column", abs("numeric_column"))
abs_df.show()
