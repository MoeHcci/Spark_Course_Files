#Example 3: Create a Spark script that filters for temp

#1. The boilerplate code for every Spark program
from pyspark import SparkConf, SparkContext
import collections

import os
path = os.getcwd()
print(path)
print(type(path))


#2. Creating the spark context & configure the context
conf = SparkConf().setMaster("local").setAppName("MinTemp")
    #Needed to configure the SparkContext
    #Allows you to choose if you want to run the work on 1 PC or a cluser, etc.
        #in this specific example:
            #.setMater("local")" --> This means set the master node where the work is exectured as the local machine
                #This covers where to run the code 
            #.setAppName("weather") --> This setting the name for this task

sc = SparkContext(conf = conf) #Assigning the configuration object into a context and we always call it "sc"
    #This is the sc file that will transfrom data into an RDD

#3. Loading up the datafile & creating an RDD from the loaded data
lines = sc.textFile ("/home/mohamd/Documents/web_dev/Spark_Python/1800.csv")

#4. Exectue commands on the RDD object

#The below function takes in the data from the CSV and returns it into a workable format 
def parseLine (x): 
    fields = x.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3])*0.1
    return (stationID, entryType,temperature )

#The below convers the dictionary to an RDD. Here the map applies the function parseLine to every element in the lines rdd
rdd = lines.map(parseLine)

#The below does mathematical calculations that involve the usage of filter. It will only return the rows that include 'TMIN' in x[1]
minTemps = rdd.filter(lambda x: 'TMIN' in x[1])
stationTemps = minTemps.map(lambda x: (x[0],x[2]))
minTemps = stationTemps.reduceByKey(lambda x,y: min(x,y))

#We must use .collect() to be able to return the data or print it as written below
results = minTemps.collect()

print ("\n")
print ("\n")
print ("\n")
print ("\n")
for x in results:
    print (x)
print ("\n")
print ("\n")
print ("\n")
print ("\n")