import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType 


spark = SparkSession.builder\
    .master("local[1]")\
    .appName("CsvToParquet")\
    .getOrCreate()
    

df = spark.read.csv("csv/zone_lookups.csv")

df = df.withColumnRenamed("_c0", "LocationID")
df = df.withColumnRenamed("_c1", "Borough")
df = df.withColumnRenamed("_c2", "Zone")
df = df.withColumnRenamed("_c3", "service_zone")

df.show()

df.write.parquet("csv/zone_lookups.parquet") 

