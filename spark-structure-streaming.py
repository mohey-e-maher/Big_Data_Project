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
    for i in row:
        print('&&&&&&&&&&&& phpmyadmin Launched with ROW value ', i)
  
    # Establish a connection to the database
    host = "localhost"
    port = 3306
    database = "output_db"
    username = "root"
    password = ""   
    conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=database)
    cursor = conn.cursor()

  # Extract the required columns from the row
    column1_value = row.rank
    column2_value = row.timestamp
    column3_value = row.rating
    column4_value = row.userId

    # Prepare the SQL query to insert data into the table
    sql_query = f"INSERT INTO result (min_rank,timestamp_avg,max_rating,userId) VALUES ({column1_value},{column2_value},{column3_value},{column4_value})"

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
schema = StructType().add("genres", StringType()).add(
    "movieId", IntegerType()).add(
    "rank", FloatType()).add(
    "rating", FloatType()).add(
    "timestamp", IntegerType()).add(
    "title", StringType()).add(
    "userId", IntegerType())
    



# Read data from Kafka topic as a DataFrame
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data"))


print('&&&&&&&&&&&&&&&& ReadStream: ', df.printSchema(), ' &&&&&&&&&&&Columns', df.columns)

# df = df.select("data.movieId","data.title","data.genres")
# print('&&&&&&&&&&&&&&&& Select: ', df.printSchema())

# After reading from Kafka, flatten the DataFrame
#########################################################################################
df = df.groupBy("data.userId").agg(
    round(min("data.rank"), 2).alias("rank"),
    round(mean("data.timestamp"), 2).alias("timestamp"),
    round(max("data.rating"), 2).alias("rating")
)
print('&&&&&&&&&&&&&&&& GroupBy:', df.printSchema(), ' &&&&&&&&&&&Columns', df.columns)

#########################################################################################

# Select specific columns from "data"
# df = df.select("data.movieId", "data.title", "data.timestamp")



# Convert the value column to string and display the result
query = df.writeStream \
    .outputMode("complete") \
    .format("console")\
    .foreach(insert_into_phpmyadmin) \
    .start()


# Wait for the query to finish
query.awaitTermination()


