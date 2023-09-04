# Big Data Analytics using Apache Spark
In this project, I used Apache Spark over a [Hadoop Distributed File System (HDFS)](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html) across 3 nodes in order to query [New York City taxi trip data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) of the months between Janury 2022 and June 2022.
In order to access the Spark API, I used [PySpark](https://spark.apache.org/docs/latest/api/python/index.html), the Python API for Apache Spark using both the [Dataframe/SQL API](https://spark.apache.org/docs/latest/sql-programming-guide.html#dataframes) and the [RDD API](https://spark.apache.org/docs/latest/rdd-programming-guide.html).

I performed 5 different queries to the data, namely:
1. Find the taxi trip with the biggest tip on the month of March and place of arrival 'Battery Park'.
2. Find, for every month, the trip with the biggest amount paid on tolls.
3. Find, for every 15 days, the average of trip distance and cost for all trips with place of arrival different from the pickup point.
4. Find the top 3 rush hours (e.g.7-8am, 3-4 pm, etc) for every day of the week with the biggest number of passenger in a taxi trip. 
5. Find the top 5 days for every month, in which the trips had the biggest tip percentage. For example, if the taxi trip fare amount was 10\$ and the tip was 5\$, then the percentage is 50%.

For every query I used the Dataframe/SQL API except for the 3rd query for which I used both the Dataframes/SQL API and the RDD API.

In the [/scripts folder](https://github.com/johnpalaios/atds-project/tree/main/scripts), you can find the code used in order to transform the data from CSV to [Parquet](https://en.wikipedia.org/wiki/Apache_Parquet) format, loading the parquet files to HDFS and then filtering them.

In the [/src folder](https://github.com/johnpalaios/atds-project/tree/main/src), you can find the source code used for query execution.

This project was made for the graduate course '[Advanced Topics in Database Systems](https://www.ece.ntua.gr/en/undergraduate/courses/3189)' (9th Semester) of the school of Electrical and Computer Engineering in the National Technical University of Athens.
