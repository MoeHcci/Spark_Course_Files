# Find the minimum temp  per station

from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql.functions import round, col
import pyspark.sql.functions as F


# 1. create the spark Session or activate an existing one
    # getorCreate() creates a SparkSession connects an already existing spark session
spark = SparkSession.builder.appName('MinTempPerYrPerStation').getOrCreate()
    # the appName can be anything -->  Sets a name for the application, which will be shown in the Spark web UI.

# 2. Read the csv file and turn it into a data frame. If the headers do not have names they will be called _c#
df = spark.read.csv('1800.csv', header=False)

# The columns have no headers. Therefore, add headers to them
df = df.toDF('station', 'date', 'type_of_reading','reading', 'name5', 'name6','name7', 'name8')

# Drop the useless columns
df = df.drop('name4', 'name5', 'name6','name7', 'name8')
# Check the types of columns
df.printSchema()

# The reading columns is not a numerical column therefore, convert it to numerical so we can apply analysis on it
df = df.withColumn( "reading",col("reading").cast("int"))

# Check the types of columns
df.printSchema()

'''
The minimum temp analysis per station 
'''
# Filter by 'TMIN'
df_min = df.filter(df.type_of_reading == 'TMIN')
# Group by the weather station
df_min = df_min.groupby('station').agg(F.min('reading'))
# show only the lowest two temps
df_min.show()

'''
The maximum temp analysis per station 
'''
# Filter by 'TMAX'
df_max = df.filter(df.type_of_reading == 'TMAX')
# Group by the weather station
df_max = df_max.groupby('station').agg(F.max('reading'))
# show only the lowest two temps
df_max.show()

# 4. The Spark session must be stopped so the next time this script runs it again getOrCreate a spark session
spark.stop() # We need this at the end of a spark script to ensure we stop it