from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,StructField,IntegerType,DateType
from pyspark.sql.functions import upper,lower
from pyspark.sql.functions import substring
from pyspark.sql.functions import to_date
spark = SparkSession.builder.appName("data").getOrCreate()
df = spark.read.option("header","true").option("inferschema","true").csv('C:/Users/RajaMahalakshmiB/Desktop/practice/practice/pyspark/resource_file/customers_100.csv')
df.show()
