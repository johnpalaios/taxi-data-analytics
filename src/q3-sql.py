from pyspark.sql import SparkSession
import datetime, sys
from pyspark.sql.functions import *
import time

def third_query_df() : 
        spark = SparkSession\
        .builder\
        .master("spark://192.168.0.1:7077") \
        .appName("third_query_df") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
        
        initialTime = time.time()

        taxiTripsDf = spark.read.parquet("hdfs://master:9000/data/taxi_trips/*.parquet")

        taxiTripsDf = taxiTripsDf \
        .filter((year(col("tpep_pickup_datetime")) == 2022) & (month(col("tpep_pickup_datetime")) <= 6))

        taxiTripsDf15 = taxiTripsDf.withColumn('15DayTimePeriod',floor(dayofyear(taxiTripsDf.tpep_pickup_datetime)/15+1))
        taxiTripsDf15 = taxiTripsDf15.filter(taxiTripsDf15.DOLocationID != taxiTripsDf15.PULocationID)
        taxiTripsDf15 = taxiTripsDf15\
                        .groupBy(col("15DayTimePeriod"))\
                        .agg(
                            avg("Total_amount").alias("average_cost"),
                            avg("Trip_distance").alias("average_distance"))\
                        .orderBy(col("15DayTimePeriod"))\
                        .show()
        
        taxiTripsDf15.write.option("header", True).csv(
        "results/third-query-sql")

        finalTime = time.time()
        return finalTime - initialTime


if __name__ == "__main__": 
        print("Going to execute the Third Query In Dataframe/SQL API...")
        print("This is the time for the Third Query In Dataframe/SQL API : " + str(third_query_df()))
