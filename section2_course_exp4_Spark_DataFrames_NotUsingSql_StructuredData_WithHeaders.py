# Find the number of friends per each age

# 0. imports required to allow us to interact with the DataFrames and Spark SQL (i.e., boilerplate)
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql.functions import round, col

# 1. create the spark Session or activate an existing one
    # getorCreate() creates a SparkSession connects an already existing spark session
spark = SparkSession.builder.appName('AvgNumFriends').getOrCreate()
    # the appName can be anything -->  Sets a name for the application, which will be shown in the Spark web UI.

# 2. Create the Data Frame
df = spark.read.csv('fakefriends-header.csv', header = True, inferSchema=True)

# 2.a Apply functions on the Df
# Find the average of the frineds column per age column
df_grouped = df.groupby('age').agg(avg("friends"))

# Renaming a column
df_grouped = df_grouped.withColumnRenamed("avg(friends)", "Avg_Friends_Per_Age")

# Rounding the "Avg_Friends_Per_Age" column to two decimal places
df_grouped = df_grouped.select("*",round("Avg_Friends_Per_Age",0))

# Dropping the addtional columns
df_grouped = df_grouped.drop('Avg_Friends_Per_Age')

# Sort by the age column
df_grouped = df_grouped.sort(df_grouped.age.asc())

# Renaming a column
df_grouped = df_grouped.withColumnRenamed("round(Avg_Friends_Per_Age, 0)", "Avg_Friends_Per_Age")

# 3. Show the dataframe
df_grouped.show()

# 4. The Spark session must be stopped so the next time this script runs it again getOrCreate a spark session
spark.stop() # We need this at the end of a spark script to ensure we stop it