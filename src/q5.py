from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
import time

def fifth_query() :
        spark = SparkSession.builder.master("spark://192.168.0.1:7077")\
                .config("spark.executor.memory", "4g")\
                .appName("fifth_query")\
                .getOrCreate()

        startTime = time.time()

        taxiTripsDf = spark.read.parquet(
            "hdfs://master:9000/data/taxi_trips/*.parquet")

        # We add one column for the tip to fare ratio percentage, one for month
        # and one for day of month
        taxiTripsDf = taxiTripsDf.withColumn("ratio", (taxiTripsDf.tip_amount/taxiTripsDf.fare_amount)*100.0)       
        taxiTripsDf = taxiTripsDf.withColumn("month", month(taxiTripsDf.tpep_pickup_datetime))       
        taxiTripsDf = taxiTripsDf.withColumn("day", dayofmonth(taxiTripsDf.tpep_pickup_datetime))       

        #now we only want these 3 columns to continue
        newDf = taxiTripsDf.select(taxiTripsDf.ratio, taxiTripsDf.month, taxiTripsDf.day)

        # Now we want to do the partition and take first three for each day of the week
        windowSpec = Window.partitionBy(taxiTripsDf.month).orderBy(taxiTripsDf.ratio.desc())
        taxiTripsDf = taxiTripsDf.withColumn("rank", row_number().over(windowSpec))
        resultDf = taxiTripsDf.filter(taxiTripsDf.rank <= 5).orderBy([taxiTripsDf.day, taxiTripsDf.rank])

        resultDf.show()

        resultDf.write.option("header", True).csv(
        "results/fifth-query")
        endTime = time.time()
        return endTime - startTime
        
if __name__ == "__main__": 
        print("Going to execute the Fifth Query In Dataframe/SQL API...")
        print("This is the time for the Fifth Query : " + str(fifth_query()))