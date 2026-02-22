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

#STRINg functon(initcap)
print("----------STRING FUNCTION INITCAP-------------------")
df.withColumn('initcap_city', initcap(col("City"))).show(2, vertical=True)

#Date function (current date)
print("----------DATE FUNCTION CURRENT_DATE-------------------")
df.withColumn('current_date', current_date()).show(2, vertical=True)

#Date_add
print("----------DATE FUNCTION DATE_ADD-------------------")
df.withColumn('date_after_7_days', date_add(current_date(), 7)).show(2, vertical=True)

#date_sub
print("----------DATE FUNCTION DATE_SUB-------------------")
df.withColumn('date_before_7_days', date_sub(current_date(), 7)).show(2, vertical=True)

#datediff
print("----------DATE FUNCTION DATEDIFF-------------------")
df.withColumn('days_diff_from_sub_to_present', datediff(current_date(), col("Subscription Date"))).show(2, vertical=True)
#date_format
print("----------DATE FUNCTION DATE_FORMAT-------------------")
df.withColumn('formatted_subscription_date', date_format(col("Subscription Date"), "MM/dd/yyyy")).show(2, vertical=True)


#----------------HANDLING NULL VALUES-------------------
print("----------HANDLING NULL VALUES-------------------")
#drop null values
'''

df.dropna('all').display() #Drops rows where ALL values are null
     

df.dropna('any').display() #Drops rows where ANY value is null
     

df.dropna(subset=['Outlet_Size']).display() #Drops rows where 'Outlet_Size' is null
     
'''
df.dropna().show(2, vertical=True)

#Filling null values with a specific value
print("----------FILLING NULL VALUES-------------------")
df.fillna("Unknown").show(2, vertical=True)

#Split and Indexing
print("----------SPLIT AND INDEXING-------------------")
#Slpit the 'Company' column into an array of words and then extract the first word
df.withColumn('Company_First_Word', split(col("Company"), " ").getItem(0)).show(2, vertical=True)

#Explode 
print("----------EXPLODE-------------------")
'''

Key Functions:
explode(): Converts arrays to separate rows
array_contains(): Checks if an array contains a specific value (returns boolean)
split(): Converts strings to arrays

1. Exploded Version:
 
df_exp.withColumn('Outlet_Type',explode('Outlet_Type')).display()
Output: Creates separate rows for each array element

diff
Copy code
+------------------+
|Outlet_Type       |
+------------------+
|Grocery           |
|Store             |
|Supermarket       |
|Type1             |
|Mall              |
|Outlet            |
+------------------+
'''
# Step 1: Split the company names
Splited_df = df.withColumn("Company", split(col("Company"), "-"))
print("=== SPLIT RESULT ===")
Splited_df.show(2, vertical=True)

# Step 2: Explode the arrays
exploded_df = Splited_df.withColumn("Company", explode(col("Company")))
print("=== EXPLODED RESULT ===")
exploded_df.show(4, vertical=True)  # Show 4 to see the explosion effect
spark.stop()