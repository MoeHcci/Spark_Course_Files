

# 0. imports required to allow us to interact with the DataFrames and Spark SQL (i.e., boilerplate)
from pyspark.sql import SparkSession
from pyspark.sql import Row

# 1. create the spark Session or activate an existing one
    # getorCreate() creates a SparkSession connects an already existing spark session
spark = SparkSession.builder.appName('sparkSQL').getOrCreate()
    # the appName can be anything -->  Sets a name for the application, which will be shown in the Spark web UI.

# 2.2 Load the CSV data into Spark. No need to convert to RDD it is already a DataFrame
people = spark.read.option("header","true").option("inferSchema", "true")\
    .csv("fakefriends-header.csv")
    # .option("header","true") --> indicates we have a header row
    # option("inferSchema", "true")--> tells spark to figure out the schema (i.e., settings) of the file
        # (Spark should be able to distinguish the headers from the normal data

# 3. Examples of commands we can run on the dataFrame
print('\n')
print('\n')
print('\n')
# This is the same as pd.info() in Pandas
print('The inferred schema is: ')
people.printSchema()
print('\n')
print('\n')
print('\n')
print('The name column is: ')
people.select('name').show()
print('\n')
print('\n')
print('\n')
print('Filter by people under 21')
people.filter(people.age <21).show()
print('\n')
print('\n')
print('\n')
print('Grouping by age')
people.groupBy('age').count().show()
print('\n')
print('\n')
print('\n')
print('Add 10 years to all people ')
people.select(people.name, people.age + 10).show()
print('\n')
print('\n')
print('\n')

# 4. The Spark session must be stopped so the next time this script runs it again getOrCreate a spark session
spark.stop() # We need this at the end of a spark script to ensure we stop it