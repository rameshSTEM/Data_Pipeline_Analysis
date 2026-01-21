import os
from pyspark.sql import SparkSession

# These packages allow Spark to talk to Kafka and Postgres
# The versions should match your Spark version (3.5)
SUBMIT_ARGS = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.postgresql:postgresql:42.7.2 pyspark-shell"
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS

spark = SparkSession.builder \
    .appName("CryptoPipeline") \
    .config("spark.driver.memory", "2g") \
    .get_status() \
    .getOrCreate()

print("Spark Session Created Successfully with Kafka & Postgres support!")