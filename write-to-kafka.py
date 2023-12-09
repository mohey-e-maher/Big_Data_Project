from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, IntegerType,FloatType

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# Define the schema for your DataFrame
schema = StructType().add("id", IntegerType()).add(
    "type", StringType()).add(
    "title", StringType()).add(
    "director", StringType()).add(
    "country", StringType()).add(
    "date_added", StringType()).add(
    "release_year", StringType()).add(
    "rating", StringType()).add(
    "duration", StringType()).add(
    "listed_in", StringType())


# Read data from a directory as a streaming DataFrame
streaming_df = spark.readStream \
    .format("json") \
    .schema(schema) \
    .option("path", "D:\coding\SA project\Big_Data_Project") \
    .load() \

# Select specific columns from "data"
df = streaming_df.select(to_json(struct("*")).alias("value"))

# Convert the value column to string and display the result
query = df.selectExpr("CAST(value AS STRING)") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "test") \
    .option("checkpointLocation", "null") \
    .start()

# Wait for the query to finish
query.awaitTermination()
