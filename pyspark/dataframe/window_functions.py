from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, lead, lag
from pyspark.sql import Row
spark = SparkSession.builder.appName("data").getOrCreate()
data = [("Raja", 100),
        ("Maha", 150),
        ("Lakshmi", 200),
        ("Ammu", 180),
        ("Dhoni", 220)]

df = spark.createDataFrame(data, ["Name", "Score"])
windowSpec = Window.orderBy("Score")

#row number
result_df = df.withColumn("RowNumber", row_number().over(windowSpec))
result_df.show()

#rank
result_df = df.withColumn("Rank", rank().over(windowSpec))
result_df.show()

#dense rank
result_df = df.withColumn("DenseRank", dense_rank().over(windowSpec))
result_df.show()

#lead
result_df = df.withColumn("LeadScore", lead("Score", 1).over(windowSpec))
result_df.show()

#lag
result_df = df.withColumn("LagScore", lag("Score", 1).over(windowSpec))
result_df.show()

