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
#.  .load('customers-100000.csv') #for 100k rows

df.show(20 ,vertical=True)

#Update the schema type  from int to string 
df_cleaned = df.withColumn(
    "Index", 
    col("Index").cast(StringType())
)
df.printSchema()
print("CHANGED SCHME TYPE DATA ")
df_cleaned.printSchema()

 

#Alias 
df_alias = df.withColumnRenamed("Index", "ID  ")
df_alias.show(2, vertical=True)


#Filter
df.filter((col("City") == "East Leonard") & (col("Index") == "1")).show(2, vertical=True)

#With col (already in 1df.py)
df.withColumn("Full Name", concat(col("First Name"), lit(" "), col("Last Name"))).show(2, vertical=True)
 

#Order by
df.orderBy(col("City").desc()).show(4 ,vertical=True)

#limit (like in sql) (limited to 3 rows)
df.limit(3).show(100, vertical=True)

#Drop column
print("----------DROP COLUMN CITY-------------------")
df.drop("City").show(2, vertical=True)

#Drop duplicates
print("----------DROP DUPLICATES in a col -------------------")
df.dropDuplicates(["City"]).show(2, vertical=True)

spark.stop()