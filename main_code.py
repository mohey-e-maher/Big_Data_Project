import pyspark
import pandas as pd
pd.read_csv('test1.csv')
from pyspark.sql import SparkSession
spark= SparkSession.builder.appName('practise').getOrCreate()
df_pyspark=spark.read.csv('test1.csv')

