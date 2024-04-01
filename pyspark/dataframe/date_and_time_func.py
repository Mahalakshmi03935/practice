from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, current_timestamp, date_add, datediff, year, month, dayofmonth, to_date, date_format
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType

spark = SparkSession.builder.appName("DateTimeFunctions").getOrCreate()

schema = StructType([
    StructField("date_column", DateType(), True),
    StructField("timestamp_column", TimestampType(), True),
])

data = [( "2022-01-01", "2022-01-01 10:00:00"), 
        ("2022-02-01", "2022-02-01 12:00:00")]

df = spark.createDataFrame(data, schema)
df.show()


# current_date()
current_date_df = df.withColumn("current_date", current_date())
print("DataFrame after current_date():")
current_date_df.show()

# current_timestamp()
current_timestamp_df = df.withColumn("current_timestamp", current_timestamp())
print("DataFrame after current_timestamp():")
current_timestamp_df.show()

# date_add()
date_add_df = df.withColumn("date_add", date_add(df["date_column"], 7))  
print("DataFrame after date_add():")
date_add_df.show()

# datediff()
datediff_df = df.withColumn("datediff", datediff(df["date_column"], to_date("2022-01-01"))) 
print("DataFrame after datediff():")
datediff_df.show()

# year()
year_df = df.withColumn("year", year(df["date_column"]))
print("DataFrame after year():")
year_df.show()

# month()
month_df = df.withColumn("month", month(df["date_column"]))
print("DataFrame after month():")
month_df.show()

# dayofmonth()
dayofmonth_df = df.withColumn("dayofmonth", dayofmonth(df["date_column"]))
print("DataFrame after dayofmonth():")
dayofmonth_df.show()

# to_date()
to_date_df = df.withColumn("date_only", to_date(df["timestamp_column"]))
print("DataFrame after to_date():")
to_date_df.show()

# date_format()
date_format_df = df.withColumn("formatted_date", date_format(df["date_column"], "yyyy-MM-dd"))
print("DataFrame after date_format():")
date_format_df.show()


