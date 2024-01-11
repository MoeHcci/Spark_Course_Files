
# 0. imports required to allow us to interact with the DataFrames and Spark SQL (i.e., boilerplate)
from pyspark.sql import SparkSession
from pyspark.sql import Row

# 1. create the spark Session or activate an existing one
    # getorCreate() creates a SparkSession connects an already existing spark session
spark = SparkSession.builder.appName('APP_NAME_MOHAMD').getOrCreate()
    # the appName can be anything -->  Sets a name for the application, which will be shown in the Spark web UI.

# 2. A function that will allow us to read a non-strctured data (i.e., a csv file without titles)
# 2.1 A decoding function
def mapper (line):
    fields = line.split(',')
    return Row(ID = int(fields[0]), name = str(fields[1].encode("utf-8")), \
                        age = int(fields[2]), numFriends = int(fields[3]))
    # This function indicated that:
        # input 0's header's name is ID, Input 1's header's name is name,
        # Input 2's header's name is age, Input 3's header's name is numFriends

# 2.2 Load the CSV data into an RDD
lines = spark.sparkContext.textFile("fakefriends.csv") # loading the file as an RDD
people = lines.map(mapper) # applying the function to every row in the RDD

# 3. Convert the RDD into a Spark DataFrame
schemaPeople = spark.createDataFrame(people).cache()
    # createDataFrame --> creates the data frame and takes in an RDD
    # .cache() --> The DataFrame must be cached to enable the running of SQL quaries on it (We need to keep it in memory)
schemaPeople.createOrReplaceTempView("people")
    # Converting the DataFrame to a DataBase table by creating or replacing an existing DataBase
    # The name does not matter
    # Note: "people" is the name that we will use SQL with

# 4. Running SQL commands on the "Table" from the DataBase (part of the DataFrame)
teenagers = spark.sql("SELECT * fROM people WHERE age >= 13 AND age <=19")
    # age was defined in the mapping function (The function we used to give titles to the unstructured CSV data)

# 5. # the below is just an example of printing the results. By using the collect() method
for teen in teenagers.collect():
    print (teen)


# 6. The Spark session must be stopped so the next time this script runs it again getOrCreate a spark session
spark.stop() # We need this at the end of a spark script to ensure we stop it