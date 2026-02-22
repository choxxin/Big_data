#Grouping data 
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DataFrameExample") \
    .master("local[*]") \
    .getOrCreate()

df = spark.createDataFrame([
    ['red', 'banana', 1, 10], ['blue', 'banana', 2, 20], ['red', 'carrot', 3, 30],
    ['blue', 'grape', 4, 40], ['red', 'carrot', 5, 50], ['black', 'carrot', 6, 60],
    ['red', 'banana', 7, 70], ['red', 'grape', 8, 80]], schema=['color', 'fruit', 'v1', 'v2'])
df.show()
#typical groub by on basis of color and show 
print("GROUP BY COLOR")
df.groupby('color').avg().show()


#You can also apply a Python native function against each group by using pandas API.
def plus_mean(pandas_df):
    return pandas_df.assign(v1=pandas_df.v1 - pandas_df.v1.mean())

df.groupby('color').applyInPandas(plus_mean, schema=df.schema).show()
'''
📊 EXAMPLE BREAKDOWN:

Original Red Group:
 
v1 values: [1, 3, 4, 6, 7]
Mean of red group: 4.2
After plus_mean for Red Group:

less
Copy code
Original v1: [1,   3,   4,   6,   7]
- Group Mean: [4.2, 4.2, 4.2, 4.2, 4.2]  
= New v1:     [-3.2, -1.2, -0.2, 1.8, 2.8]

'''
def plus_mean(pandas_df):
    return pandas_df.assign(v1=pandas_df.v1 - pandas_df.v1.mean())

df.groupby('color').applyInPandas(plus_mean, schema=df.schema).show()


#Getting the data OUT #csv

# df.write.csv('foo.csv',header = True)
# print("DATA from CSV BELOW")
# spark.read.csv('foo.csv' ,header=True).show()

#Parquet (VERY efficient compared to parquet)


# df.write.parquet('bar.parquet')
# spark.read.parquet('bar.parquet').show()

#Another is ORC Type 

#-------Working in sql-----------------

df.createOrReplaceTempView("tableA")
spark.sql("SELECT count(*) from tableA").show()