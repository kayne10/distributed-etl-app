import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Configure Spark to use the Standalone Cluster Manager
# Maybe try this .master("spark://etl-worker-1:7077,etl-worker-2:7078,etl-worker-3:7079")
spark = SparkSession \
    .builder \
    .appName("Top 10 Net Mask Wear Score") \
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

try:
    # Extract tables in dataframe with partition column if possible
    county_pop = spark.read.jdbc(
        url=postgres_url,
        table="county_population",
        properties=properties
    )
    county_pop.printSchema()

    mask_use = spark.read.jdbc(
        url=postgres_url,
        table="mask_use_by_county",
        properties=properties
    )
    mask_use.printSchema()

    # Merge and process dataframes
    joined_df = county_pop.join(mask_use, "fips", "inner")

    joined_df = joined_df.withColumn("would_not_wear",F.col("never") + F.col("rarely"))
    joined_df = joined_df.withColumn("would_wear", F.col("frequently") + F.col("always"))
    joined_df = joined_df.withColumn("nmw_score",
                        (F.col("would_wear") - F.col("would_not_wear")) * F.col("population_estimate_2020"))
    
    results = joined_df.orderBy(F.col("nmw_score").desc()).limit(10)
    results = results.withColumn("rank",F.monotonically_increasing_id()+1)
    results = results.select("rank","fips","county_name","state_name","would_wear","would_not_wear","population_estimate_2020","nmw_score")
    
    custom_header = ["Rank","FIPS","County","State","Share Would Wear","Share Would Not Wear","Population","Net Mask Wearer Score"]
    output_path = '/tmp/reports/top_10_nmw_score.csv'
    results.toPandas().to_csv(output_path, header=custom_header, index=False)

except Exception as e:
    print(e)
finally:
    spark.stop()