from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder.appName("data").getOrCreate()

data1 = [("Raja", 34), ("Maha", 45), ("Lakshmi", 50)]
columns1 = ["Name", "Age"]
df1 = spark.createDataFrame(data1, columns1)
print("The dataframe 1 is :")
df1.show()

data2 = [("Raja", "Medical"), ("Maha", "Engineering"), ("Lakshmi", "Marketing")]
columns2 = ["Name", "Department"]
df2 = spark.createDataFrame(data2, columns2)
print("The dataframe 2 is :")
df2.show()

#innerjoin
df_inner_join = df1.join(df2, on='Name', how='inner')
df_inner_join.show()

#outerjoin
df_outer_join = df1.join(df2, on='Name', how='outer')
df_outer_join.show()

#crossjoin
df_cross_join = df1.crossJoin(df2)
df_cross_join.show()

#leftjoin
df_left_join = df1.join(df2, on='Name', how='left')
df_left_join.show()

#rightjoin
df_right_join = df1.join(df2, on='Name', how='right')
df_right_join.show()

#leftantijoin

df_left_anti_join = df1.join(df2, on='Name', how='left_anti')
df_left_anti_join.show()

#rightantijoin

df_right_anti_join = df1.join(df2, on='Name', how='right_anti')
df_right_anti_join.show()

#selfjoin

df_self_join = df1.alias('df1').join(df1.alias('df2'), on='Age', how='inner')
df_self_join.show()

#broadcastjoin

df_broadcast_join = df1.join(broadcast(df2), on='Name', how='inner')
df_broadcast_join.show()






