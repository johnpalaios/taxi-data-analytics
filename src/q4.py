from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
import time


def FourthQuery() :
        spark = SparkSession.builder.master("spark://192.168.0.1:7077")\
                .config("spark.executor.memory", "4g")\
                .appName("fourth-query").getOrCreate()

        startTime = time.time()

        taxiTripsDf = spark.read.parquet(
            "hdfs://master:9000/data/taxi_trips/*.parquet")

        # We add 2 columns (one for the hour and one for the day)
        taxiTripsDf = taxiTripsDf.withColumn("hour_of_day", hour(taxiTripsDf.tpep_pickup_datetime))       
        taxiTripsDf = taxiTripsDf.withColumn("day_of_week", dayofweek(taxiTripsDf.tpep_pickup_datetime))       
        
        # We group by the two columns we created before and we sum the passenger count
        taxiTripsDf = taxiTripsDf.groupBy(taxiTripsDf.hour_of_day, taxiTripsDf.day_of_week)\
                    .agg(sum(taxiTripsDf.passenger_count).alias("passenger_sum"))
        
        
        # Now we want to do the partition and take first three for each day of the week
        windowSpec = Window.partitionBy(taxiTripsDf.day_of_week).orderBy(taxiTripsDf.passenger_sum.desc())
        taxiTripsDf = taxiTripsDf.withColumn("rank", row_number().over(windowSpec))
        resultDf = taxiTripsDf.filter(taxiTripsDf.rank <= 3).orderBy([taxiTripsDf.day_of_week, taxiTripsDf.rank])
        
        resultDf.show()

        resultDf.write.option("header", True).csv(
        "results/fourth-query")
        
        endTime = time.time()
        return endTime - startTime
        
if __name__ == "__main__": 
        print("Going to execute the Fourth Query In Dataframe/SQL API...")
        print("This is the time for the Fourth Query : " + str(FourthQuery()))