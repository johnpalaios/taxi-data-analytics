import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import datetime as dt
import time

def first_query() :
        spark = SparkSession\
                .builder\
                .master("spark://192.168.0.1:7077") \
                .appName("first_query") \
                .config("spark.driver.memory", "4g") \
                .getOrCreate()

        startTime = time.time() 

        # taxiTripsDf = spark.read.parquet("hdfs://master:9000/data/yellow_trip_data/yellow_tripdata_2022-01.parquet")
        taxiTripsDf = spark.read.parquet("hdfs://master:9000/data/taxi_trips/*.parquet")
        zoneLookupsDf = spark.read.parquet("hdfs://master:9000/data/zone_lookups.parquet")

        joinedDf = taxiTripsDf.join(zoneLookupsDf, taxiTripsDf.DOLocationID == zoneLookupsDf.LocationID, "inner")
        
        maxTipRideForMarch = joinedDf.filter(
        (month(joinedDf.tpep_pickup_datetime) == 3) & (joinedDf.Zone == "Battery Park"))\
        .orderBy(joinedDf.tip_amount, ascending=False).limit(1)
        

        maxTipRideForMarch.write.option("header", True).csv(
        "hdfs://master:9000/results/first-query")

        endTime = time.time()

        maxTipRideForMarch.show()

        # return the time
        return endTime - startTime

if __name__ == "__main__": 
        print("Going to execute the First Query...")
        print("This is the time for the firstQuery : " + str(first_query()))