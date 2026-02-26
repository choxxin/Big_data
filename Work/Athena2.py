from pyspark.sql import SparkSession
from datetime import datetime, date
from pyspark.sql import Row
from pyspark.sql import Column
from pyspark.sql.functions import upper

spark = SparkSession.builder \
    .appName("DataFrame Example") \
    .getOrCreate()

df = spark.read.format('csv') \
    .option('inferSchema', True) \
    .option('header', True) \
    .load('Athena.csv')

df.show(20 , vertical=True)

df.createOrReplaceTempView('athena')
spark.sql("SELECT * FROM athena where registration_id = 'ZEDES00525'").show(vertical=True)
spark.stop()