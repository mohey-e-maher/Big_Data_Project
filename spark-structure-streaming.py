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
    column1_value = row.type
    column2_value = row.title
    column3_value = row.director
    column4_value = row.country
    column5_value = row.date_added
    column6_value = row.release_year
    column7_value = row.rating
    column8_value = row.duration
    column9_value = row.listed_in

    # Prepare the SQL query to insert data into the table
    sql_query = f"INSERT INTO bigout (type,title,director,country,date_added,release_year,rating,duration,listed_in) VALUES ({column1_value},{column2_value},{column3_value},{column4_value},{column5_value},{column6_value},{column7_value},{column8_value},'{column9_value}')"
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
    "type", StringType()).add(
    "title", StringType()).add(
    "director", StringType()).add(
    "country", StringType()).add(
    "date_added", StringType()).add(
    "release_year", StringType()).add(
    "rating", StringType()).add(
    "duration", StringType()).add(
    "listed_in", StringType())


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
df = df.select("data.type","data.title","data.director","data.country","data.date_added","data.release_year","data.rating","data.duration","data.listed_in")


# Convert the value column to string and display the result
query = df.writeStream \
    .outputMode("append") \
    .format("console")\
    .foreach(insert_into_phpmyadmin) \
    .start()


# Wait for the query to finish
query.awaitTermination()
