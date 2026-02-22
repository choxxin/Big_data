from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
spark = SparkSession.builder \
    .appName("DataFrameExample") \
    .master("local[*]") \
    .getOrCreate()  



 
data = [('user1','book1'),
        ('user1','book2'),
        ('user2','book2'),
        ('user2','book4'),
        ('user3','book1')]

schema = 'user string, book string'

df_book = spark.createDataFrame(data,schema)

df_book.show()
'''
Thoughts


collect_list() is an aggregation function that collects values from multiple rows into a single array/list.

What it does:
Opposite of explode(): While explode splits arrays into rows, collect_list combines rows into arrays
Groups data and puts all values of a column into one array per group


'''

df_book.groupBy('user').agg(collect_list('book')).show()

#Pivot 
'''
What Pivot Does:
Takes unique values from a column and makes them new columns
Spreads data horizontally instead of vertically
'''
# Create sample sales data
data2 = [
    ("Alice", "Q1", "Electronics", 1000),
     ("Alice", "Q1", "Electronics", 1),
    ("Alice", "Q2", "Electronics", 1200),
    ("Alice", "Q1", "Clothing", 800),
    ("Alice", "Q2", "Clothing", 900),
    ("Bob", "Q1", "Electronics", 1500),
    ("Bob", "Q2", "Electronics", 1300),
    ("Bob", "Q1", "Clothing", 600),
    ("Bob", "Q2", "Clothing", 700),
    ("Charlie", "Q1", "Electronics", 2000),
    ("Charlie", "Q2", "Electronics", 2200)
]

columns2 = ["Employee", "Quarter", "Category", "Sales"]
df2 = spark.createDataFrame(data2, columns2)

print("=== ORIGINAL DATA (Long Format) ===")
df2.show()
# Pivot quarters into columns
df2_pivot_quarter = df2.groupBy("Employee", "Category") \
                       .pivot("Quarter") \
                       .agg(sum("Sales"))

print("=== PIVOTED BY QUARTER ===")
df2_pivot_quarter.show()

#=============Joins========================

dataj1 = [('1','gaur','d01'),
          ('2','kit','d02'),
          ('3','sam','d03'),
          ('4','tim','d03'),
          ('5','aman','d05'),
          ('6','nad','d06')] 

schemaj1 = 'emp_id STRING, emp_name STRING, dept_id STRING' 

df1 = spark.createDataFrame(dataj1,schemaj1)

dataj2 = [('d01','HR'),
          ('d02','Marketing'),
          ('d03','Accounts'),
          ('d04','IT'),
          ('d05','Finance')]

schemaj2 = 'dept_id STRING, department STRING'

df2 = spark.createDataFrame(dataj2,schemaj2)
     

df1.show()
     

df2.show()

#inner join
print("INNER JOIN")
df1.join(df2, on='dept_id', how='inner').show()

#left join
print("LEFT JOIN")
df1.join(df2, on='dept_id', how='left').show()

#==========Ranks========================
'''  
RANK vs DENSE_RANK
Key Difference:
RANK(): Leaves gaps in ranking when there are ties
DENSE_RANK(): No gaps in ranking when there are ties
'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Sample data
data = [
    ("Item1", 100),
    ("Item2", 200),
    ("Item3", 200),  # Duplicate value
    ("Item4", 300),
    ("Item5", 300),  # Duplicate value
    ("Item6", 400)
]

df = spark.createDataFrame(data, ["Item_Identifier", "Item_MRP"])

# Apply RANK and DENSE_RANK
window_spec = Window.orderBy(col('Item_MRP').desc())

result = df.withColumn('rank', rank().over(window_spec)) \
          .withColumn('dense_rank', dense_rank().over(window_spec))

result.show()