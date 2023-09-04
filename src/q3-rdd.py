from pyspark.sql import SparkSession
import datetime, sys
from pyspark.sql.functions import *
import time

def third_query_rdd() : 
        spark = SparkSession\
        .builder\
        .master("local[1]") \
        .appName("third_query_rdd") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
        
        initialTime = time.time()

        taxiTripsDf = spark.read.parquet("hdfs://master:9000/data/taxi_trips/*.parquet")
        taxiTripsRDD = taxiTripsDf.rdd
        
        # function to convert date to 15 day
        def convTo15DayPeriod(line):
            datetime = line.tpep_pickup_datetime
            fifteenDayPeriod = datetime.timetuple().tm_yday//15 + 1
            return (fifteenDayPeriod, (line.trip_distance, line.total_amount, 1))
        
        taxiTripsRDD = taxiTripsRDD.filter(lambda x: x.DOLocationID != x.PULocationID)
        taxiTripsRDD = taxiTripsRDD.map(convTo15DayPeriod).reduceByKey(
            lambda a, b: (a[0]+b[0], a[1]+b[1], a[2]+b[2]))
        taxiTripsRDD = taxiTripsRDD.mapValues(lambda x: (x[0]/x[2], x[1]/x[2]))
        result = taxiTripsRDD.collect() 

        for i in result :
               print(i)

        result.write.option("header", True).csv(
        "hdfs://master:9000/results/second-query-rdd")
        finalTime = time.time()
        return finalTime - initialTime


if __name__ == "__main__": 
        print("Going to execute the Third Query In RDD API...")
        print("This is the time for the Third Query In RDD API : " + str(third_query_rdd()))
