from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

spark = SparkSession.builder \
    .appName("DataFrameExample") \
    .master("local[*]") \
    .getOrCreate()

# \ reprsents next line for a continue line of code
df = spark.read.format('csv') \
    .option('inferSchema', True) \
    .option('header', True) \
    .load('customers.csv')

df.show(20 ,vertical=True)

#Update the schema type  from int to string 
df_cleaned = df.withColumn(
    "Index", 
    col("Index").cast(StringType())
)
df.printSchema()
print("CHANGED SCHME TYPE DATA ")
df_cleaned.printSchema()

spark.stop()