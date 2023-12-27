#Example 6: Create a Spar6 Script that count the spendings of customers 
# data --> customer id, item id, the amount spent on the item

#1. The boilerplate code for every Spark program
from pyspark import SparkConf, SparkContext
import collections
import re

import os
path = os.getcwd()
print(path)
print(type(path))



#2. Creating the spark context & configure the context
conf = SparkConf().setMaster("local").setAppName("customer-orders")
    #Needed to configure the SparkContext
    #Allows you to choose if you want to run the work on 1 PC or a cluser, etc.
        #in this specific example:
            #.setMater("local")" --> This means set the master node where the work is exectured as the local machine
                #This covers where to run the code 
            #.setAppName("customer-orders") --> This setting the name for this task

sc = SparkContext(conf = conf) #Assigning the configuration object into a context and we always call it "sc"
    #This is the sc file that will transfrom data into an RDD

#3. Loading up the datafile & creating an RDD from the loaded data
lines = sc.textFile ("/home/mohamd/Documents/web_dev/Spark_Python/customer-orders.csv")

#4. Exectue commands on the RDD object

def parseLine (x): 
    customer_spendings = x.split(',')
    customerID = int(customer_spendings[0])
    amountSpent = float(customer_spendings[2])
    return (customerID, amountSpent)


rdd = lines.map(parseLine) #--> This will return  --> (customer id, the amount spent on the item)


print ("\n")
print ("\n")
print ("\n")
print ("\n") 
#convert the rewsults from the RDD to two lists
keys_1 = []
values_1 = []
for x,y in rdd.collect():
    keys_1.append(x)
    values_1.append(y)

#Convert the lists to a a large tuple, because a dict structure can't have duplicate keys.
results_1 = list(zip(keys_1,values_1))


#Add all the values of the same keys tg
results_1__d = {} #create an empty dic
for key, val in results_1:
    if key in results_1__d:
        results_1__d[key] = results_1__d[key] + val #equal to the previous results plus the new value
    else:
        results_1__d[key] = val





#sort the dictionary by the values from smallest to the largest 
results_1__d = sorted(results_1__d.items(),key=lambda x:x[1] )

print (results_1__d)    



print ("\n")
print ("\n")
print ("\n")
print ("\n") 

