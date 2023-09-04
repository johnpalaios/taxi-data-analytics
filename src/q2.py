from pyspark.sql import SparkSession

import datetime, sys, os
from pyspark.sql.functions import year, month, dayofmonth

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import datetime as dt
import time

def second_query() :
        spark = SparkSession\
        .builder\
        .master("spark://192.168.0.1:7077") \
        .appName("second_query") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

        taxiTripsDf = spark.read.parquet("hdfs://master:9000/data/taxi_trips/*.parquet")

        startTime = time.time()

        taxiTripsDf = taxiTripsDf \
            .filter((year(col("tpep_pickup_datetime")) == 2022) & (month(col("tpep_pickup_datetime")) <= 6))

        taxiTripsDfMonth = taxiTripsDf.withColumn('month',month(taxiTripsDf.tpep_pickup_datetime))
        taxiTripsDfMonth = taxiTripsDfMonth.groupBy(col("month"))
        result = taxiTripsDfMonth.max("Tolls_amount")\
                            .sort(col("month"))\
                            .withColumnRenamed("max(Tolls_amount)", "Highest_Toll_Amount")



        result.write.option("header", True).csv(
        "results/second-query")

        finalTime = time.time()

        result.show()

        # return the time
        return finalTime - startTime

if __name__ == "__main__": 
        print("Going to execute the Second Query...")
        print("This is the time for the Second Query : " + str(second_query()))
