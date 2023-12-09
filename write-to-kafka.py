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
    "Formatted_Date", StringType()).add(
    "Temperature", FloatType()).add(
    "Apparent_Temperature", FloatType()).add(
    "Humidity", FloatType()).add(
    "Wind_Speed", FloatType()).add(
    "Wind_Bearing", FloatType()).add(
    "Visibility", FloatType()).add(
    "Pressure", FloatType()).add(
    "Precip_Type", StringType())


# Read data from a directory as a streaming DataFrame
streaming_df = spark.readStream \
    .format("json") \
    .schema(schema) \
    .option("path", r"D:\coding\SA project\Big_Data_Project") \
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
