#Example 1: Create a Spark Script that takes in a large set of data of movies ratings and return how many times each rating was mentioned

#1. The boilerplate code for every Spark program
from pyspark import SparkConf, SparkContext
import collections

import os
path = os.getcwd()
print(path)
print(type(path))


#2. Creating the spark context & configure the context
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
    #Needed to configure the SparkContext
    #Allows you to choose if you want to run the work on 1 PC or a cluser, etc.
        #in this specific example:
            #.setMater("local")" --> This means set the master node where the work is exectured as the local machine
                #This covers where to run the code 
            #.setAppName("RatingsHistogram") --> This setting the name for this task

sc = SparkContext(conf = conf) #Assigning the configuration object into a context and we always call it "sc"
    #This is the sc file that will transfrom data into an RDD

#3. Loading up the datafile & creating an RDD from the loaded data
lines = sc.textFile ("/home/mohamd/Documents/web_dev/Spark_Python/ml-100k/u.data")

#4. Exectue commands on the RDD object
ratings = lines.map(lambda x: x.split()[2]) #This just to extract some of the data

#5. Perfrom an action on the data
result = ratings.countByValue() #this is like the unique command in pandas

#6. Print the results
print ("\n")
print ("\n")
print ("\n")
print ("\n")
print (type(ratings))
print (result)
print ("\n")
print ("\n")
print ("\n")
print ("\n")
