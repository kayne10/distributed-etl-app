import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum

# Configure Spark to use the Standalone Cluster Manager
# Maybe try this .master("spark://etl-worker-1:7077,etl-worker-2:7078,etl-worker-3:7079")
spark = SparkSession \
    .builder \
    .appName("Daily Cases and Deaths by State") \
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
    # Extract table in dataframe with partition column
    df = spark.read.jdbc(
        url=postgres_url,
        table=table_name,
        properties=properties
    )
    df.printSchema()

    # Transform table with aggregation for daily cases and deaths by state
    results = df.groupBy("date", "state_name") \
                .agg( \
                    _sum("cases").alias("Cases"), \
                    _sum("deaths").alias("Deaths") \
                ).sort("date","state_name")
    
    custom_header = ["Date","State","Cases","Deaths"]
    output_path = '/tmp/reports/daily_cases_deaths.csv'
    results.toPandas().to_csv(output_path, header=custom_header, index=False)

except Exception as e:
    print(e)
finally:
    spark.stop()