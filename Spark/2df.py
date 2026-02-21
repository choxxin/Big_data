from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
import pandas as pd 
from pyspark.sql.functions import pandas_udf

'''
PySpark supports various UDFs and APIs to allow users to execute Python native functions. See also the latest Pandas UDFs and Pandas Function APIs. For instance, the example below allows users to directly use the APIs in a pandas Series within Python native function.


When you see .master("local[*]"), here is what’s happening under the hood:

local: This tells Spark not to look for a cluster of servers. Instead, run everything on your own machine (laptop/desktop).

[*]: The asterisk is a wildcard for your CPU cores. It tells Spark, "Use every single core available on this computer."

If you have a 4-core processor, Spark will spin up 4 threads to process data in parallel.

If you wrote local[2], Spark would only use 2 cores, even if you had 16.
'''
spark = SparkSession.builder \
    .appName("DataFrameExample") \
    .master("local[*]") \
    .getOrCreate()
# Define your custom function
   
def categorize_age(age): #Regular udf(Slow row by row)
    if age < 18:
        return "Child"
    elif age < 65:
        return "Adult"
    else:
        return "Senior"

# Convert to UDF
age_category_udf = udf(categorize_age, StringType())

# Use it on DataFrame
df = spark.createDataFrame([(25, 'John'), (70, 'Mary'), (10, 'Tim')], ['age', 'name'])
df_with_category = df.withColumn("category", age_category_udf(df.age))
df_with_category.show()



#2-------------------Pandas UDF (vectorized) optimised -----------------------
@pandas_udf('long')
def pandas_plus_one(series: pd.Series) -> pd.Series:  # Processes ENTIRE series
    return series + 1

df.select(pandas_plus_one(df.age)).show()