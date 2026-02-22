# imports 
from pyspark.sql import SparkSession
from datetime import datetime, date
from pyspark.sql import Row
from pyspark.sql import Column
from pyspark.sql.functions import upper

# Create Spark session BEFORE using 'spark'
spark = SparkSession.builder \
    .appName("DataFrame Example") \
    .getOrCreate()

# Now your existing code will work
df = spark.createDataFrame([
    Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
    Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
    Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
])

# Show the DataFrame
df.show()

df.show(1)
#show only the top row 
df.show(1 , vertical=True)
#shows the col ex a,b,c,d
df.columns
df.printSchema()
#Show the summary of the DataFrame (selects helps to show the specific col summary)
df.select("a", "b", "c").describe().show()
df.a
type(df.a)     

#Creare a new DF and combine it 
df2 = spark.createDataFrame([
    Row(a=5, b=6., c='string4', d=date(2000, 4, 1), e=datetime(2000, 1, 4, 12, 0)),
    Row(a=6, b=7., c='string5', d=date(2000, 5, 1), e=datetime(2000, 1, 5, 12, 0)),
    Row(a=7, b=8., c='string6', d=date(2000, 6, 1), e=datetime(2000, 1, 6, 12, 0))
])

combined_df= df.union(df2)
combined_df.show()


#to show the single col 
df.select(df.a).show()

#Col with new name(Assign new Column instance. )
df.withColumn('Same_as_C_just_repeat', upper(df.c)).show()
df.show()
 

#Filter 
df.filter(df.a == 1).show()  #Takes condition and show specific rows


# Stop Spark session at the endß
spark.stop()