#!/bin/bash

hadoop fs -mkdir hdfs://master:9000/data
echo "Created hdfs://master:9000/user/user/data directory in HDFS"
hadoop fs -mkdir hdfs://master:9000/data/taxi_trips/
hadoop fs -put /home/user/atds-project/data/zone_lookups.parquet hdfs://master:9000/data/
hadoop fs -put /home/user/atds-project/data/taxi_trips hdfs://master:9000/data/
echo "And this is the HDFS tree file/directory representation : "
hadoop fs -ls  hdfs://master:9000/data | awk '{print $8}' | sed -e 's/[^-][^\/]*\//--/g' -e 's/^/ /' -e 's/-/|/'
