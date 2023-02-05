from pyspark.sql import SparkSession

import datetime, sys
from pyspark.sql.functions import year, month, dayofmonth

spark = SparkSession\
        .builder\
        .appName("Make-Dataframes-And-Rdds")\
        .getOrCreate()

tripData = []

tr01 = spark.read.parquet("hdfs://master:9000/data/yellow_trip_data/yellow_tripdata_2022-01.parquet")
tr02 = spark.read.parquet("hdfs://master:9000/data/yellow_trip_data/yellow_tripdata_2022-02.parquet")
tr03 = spark.read.parquet("hdfs://master:9000/data/yellow_trip_data/yellow_tripdata_2022-03.parquet")
tr04 = spark.read.parquet("hdfs://master:9000/data/yellow_trip_data/yellow_tripdata_2022-04.parquet")
tr05 = spark.read.parquet("hdfs://master:9000/data/yellow_trip_data/yellow_tripdata_2022-05.parquet")
tr06 = spark.read.parquet("hdfs://master:9000/data/yellow_trip_data/yellow_tripdata_2022-06.parquet")

tripData.append(tr01)
tripData.append(tr02)
tripData.append(tr03)
tripData.append(tr04)
tripData.append(tr05)
tripData.append(tr06)

stringSeparator = "        ************************************************************        \n"

outputString = ""
outputString += stringSeparator
sum = 0
for i in range(len(tripData)):
    sum += tripData[i].count()
    outputString += "Count " + str(i) + " = " + str(tripData[i].count()) + "\n"
outputString += "Total Sum of the tripData List = " + str(sum) + "\n"

yellowTripData = spark.read.parquet("hdfs://master:9000/data/yellow_trip_data/*.parquet")
outputString += "This is the whole data = " + str(yellowTripData.count()) + "\n"
outputString += stringSeparator

# no null values
yellowTripData = yellowTripData.dropna()

# inside our timeperiod
yellowTripData = yellowTripData.filter(month(yellowTripData.tpep_pickup_datetime) <= 6)
yellowTripData = yellowTripData.filter(year(yellowTripData.tpep_pickup_datetime) == 2022)

# data > 0
yellowTripData = yellowTripData.filter((yellowTripData.passenger_count >= 0) &
                (yellowTripData.trip_distance >= 0) & (yellowTripData.fare_amount >= 0) &
                (yellowTripData.tip_amount >= 0) & (yellowTripData.tolls_amount >= 0))


if sum == yellowTripData.count() :
    yellowTripData.write.parquet("hdfs://master:9000/data/taxi_trips1.parquet") 
    outputString += "The unified Dataframe is written to HDFS : hdfs://master:9000/data/yellow_trip_data/yellow-trip-data-01-06.parquet"

print(outputString)
