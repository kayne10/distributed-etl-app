import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import logging as log

# Configure Spark to use the Standalone Cluster Manager
# Maybe try this .master("spark://etl-worker-1:7077,etl-worker-2:7078,etl-worker-3:7079")
spark = SparkSession \
    .builder \
    .appName("CovidEtlJob") \
    .config("spark.jars","/usr/local/spark/jars/postgresql-42.2.14.jar") \
    .master("spark://etl-master:7077") \
    .getOrCreate()

# Establish postgres connection
postgres_url = "jdbc:postgresql://postgres:5432/nyt_covid19"
properties = {
    "user": os.environ.get('PG_USER'),
    "password": os.environ.get('PG_PASS'),
    "driver": "org.postgresql.Driver"
}
table_name = "county_population"
partition_col = "county_name"
lower_bound = 'Alabama'
upper_bound = 'Wyoming'
num_partitions = 10

try:
    # Extract table in dataframe with partition column
    df = spark.read.jdbc(
        url=postgres_url,
        table=table_name,
        column=partition_col,
        lowerBound=lower_bound,
        upperBound=upper_bound,
        numPartitions=num_partitions,
        properties=properties
    )

    # Transform table with rolling average
    df.printSchema()

except Exception as e:
    print(e)
finally:
    spark.stop()