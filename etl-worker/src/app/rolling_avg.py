import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Configure Spark to use the Standalone Cluster Manager
# Maybe try this .master("spark://etl-worker-1:7077,etl-worker-2:7078,etl-worker-3:7079")
spark = SparkSession \
    .builder \
    .appName("Rolling Average of Cases and Deaths") \
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
table_name = "us_state_cumulative"

try:
    # Extract table in dataframe with partition column if possible
    df = spark.read.jdbc(
        url=postgres_url,
        table=table_name,
        properties=properties
    )
    df.printSchema()
    
    # Transform table with rolling average
    window_spec = Window.partitionBy("state_name").orderBy("date").rowsBetween(-7, 0)
    df = df.withColumn("rolling_avg_cases", F.avg(F.col("cases")).over(window_spec)) \
            .withColumn("rolling_avg_deaths", F.avg(F.col("deaths")).over(window_spec))
    df = df.select("date","state_name","rolling_avg_cases","rolling_avg_deaths")
    
    custom_header = ["Date","State","Rolling Avg Cases","Rolling Avg Deaths"]
    output_path = '/tmp/reports/rolling_avg_cases_deaths.csv'
    df.toPandas().to_csv(output_path, header=custom_header, index=False)

except Exception as e:
    print(e)
finally:
    spark.stop()