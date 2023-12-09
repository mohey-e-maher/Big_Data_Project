from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType, IntegerType,FloatType
import pymysql
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'
from pyspark.sql.functions import col, count,desc,min,mean
from pyspark.sql.types import StructType, StructField, StringType, FloatType



    # fun to Define the connection details for your PHPMyAdmin database

def insert_into_phpmyadmin(row):
    host = "localhost"
    port = 3306
    database = "output_db"
    username = "root"
    password = ""
    conn = pymysql.connect(host=host, port=port,user=username, passwd=password, db=database)
    cursor = conn.cursor()
    # Extract the required columns from the row
    column1_value = row.Temperature
    column2_value = row.Wind_Speed
    column3_value = row.Pressure
    column4_value = row.Precip_Type

    # Prepare the SQL query to insert data into the table
    sql_query = f"INSERT INTO user (temp_avg,max_wind,presure_avg,rain_snow) VALUES ({column1_value},{column2_value},{column3_value},'{column4_value}')"
    # Execute the SQL query
    cursor.execute(sql_query)
    # Commit the changes
    conn.commit()
    conn.close()



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


# Read data from Kafka topic as a DataFrame
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \

#########################################################################################


#########################################################################################

# Select specific columns from "data"
df = df.select("data.Temperature","data.Wind_Speed","data.Pressure","data.Precip_Type")


# Convert the value column to string and display the result
query = df.writeStream \
    .outputMode("append") \
    .format("console")\
    .foreach(insert_into_phpmyadmin) \
    .start()


# Wait for the query to finish
query.awaitTermination()
