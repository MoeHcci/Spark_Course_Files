#Example 2: Create a Spark Script that takes in a large set of people ahges and the number of frinds they got and return the average frineds # for each age

#1. The boilerplate code for every Spark program
from pyspark import SparkConf, SparkContext
import collections

import os
path = os.getcwd()
print(path)
print(type(path))


#2. Creating the spark context & configure the context
conf = SparkConf().setMaster("local").setAppName("fakefriends")
    #Needed to configure the SparkContext
    #Allows you to choose if you want to run the work on 1 PC or a cluser, etc.
        #in this specific example:
            #.setMater("local")" --> This means set the master node where the work is exectured as the local machine
                #This covers where to run the code 
            #.setAppName("fakefriends") --> This setting the name for this task

sc = SparkContext(conf = conf) #Assigning the configuration object into a context and we always call it "sc"
    #This is the sc file that will transfrom data into an RDD

#3. Loading up the datafile & creating an RDD from the loaded data
lines = sc.textFile ("/home/mohamd/Documents/web_dev/Spark_Python/fakefriends.csv")

#4. Exectue commands on the RDD object

#The below function takes in the data from the CSV and returns a dictionary 
def parseLine (x): 
    fields = x.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

#The below convers the dictionary to an RDD
rdd = lines.map(parseLine)

#The below does mathematical calculations on the values and keys of the dictionary 
totalsByAge = rdd.mapValues(lambda x: (x,1)).reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1]))
averageByAge = totalsByAge.mapValues(lambda x: x[0]/x[1])

#We must use .collect() to be able to return the data or print it as written below
results = averageByAge.collect()

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