# imports 
from pyspark.sql import SparkSession
from datetime import datetime, date
from pyspark.sql import Row
from pyspark.sql import Column
 


spark = SparkSession.builder \
    .appName("DataFrame Example") \
    .getOrCreate()

df = spark.read.format('csv') \
    .option('inferSchema', True) \
    .option('header', True) \
    .load('amazon_raw_data.csv')

df.show(20)

df.createOrReplaceTempView("tableA")
spark.sql("Select count(*) from tableA where devicetime >= " \
" '2025-12-29 17:49:03.000' and devicetime <= '2025-12-29 24:00:00.000' ").show()

spark.stop()