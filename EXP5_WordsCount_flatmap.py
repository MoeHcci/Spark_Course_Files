#Example 5: Create a Sparl Script that counts the words in a text file

#1. The boilerplate code for every Spark program
from pyspark import SparkConf, SparkContext
import collections
import re

import os
path = os.getcwd()
print(path)
print(type(path))







#2. Creating the spark context & configure the context
conf = SparkConf().setMaster("local").setAppName("book")
    #Needed to configure the SparkContext
    #Allows you to choose if you want to run the work on 1 PC or a cluser, etc.
        #in this specific example:
            #.setMater("local")" --> This means set the master node where the work is exectured as the local machine
                #This covers where to run the code 
            #.setAppName("book") --> This setting the name for this task

sc = SparkContext(conf = conf) #Assigning the configuration object into a context and we always call it "sc"
    #This is the sc file that will transfrom data into an RDD

#3. Loading up the datafile & creating an RDD from the loaded data
lines = sc.textFile ("/home/mohamd/Documents/web_dev/Spark_Python/Book.txt")

#4. Exectue commands on the RDD object

#function that replaces anything that is not a word or a white space by ' '. then, makes the words into lower case
def normalize_lower_word (x):
    x = re.sub(r'[^\w\s]', ' ', x)   #sub anything that is not \w or \s by ' '. this will leave words & numbers alone
    return x.lower()    

def splitting_lowercase(x):
    return x.split() #split via white space

rdd = lines.map(normalize_lower_word) #---> replace anything that is not a word or a white space by ' '. Also, it changes it to lower case. For every word it returns 1 word (i.e., using map())
rdd = rdd.flatMap(splitting_lowercase) #---> sublit via the white space. For every sentence we return multiple words (i.e., using flatmap)
wordCounts = rdd.countByValue() #spark method to count how manytimes each value been repeated 


print ("\n")
print ("\n")
print ("\n")
print ("\n") 
keys_list = [] # A list for all the keys
values_list = [] # a list for al the values
for x, y in wordCounts.items():
    x = x.encode('ascii', 'ignore') #converting from unicode to ascii and ignoring  all the errors
    keys_list.append(x)
    values_list.append(y)

#A function that takes in two lists and returns a list of tuples of those two lists. 
def merge(list1, list2):
 
    merged_list = [(list1[i], list2[i]) for i in range(0, len(list1))]
     
    return merged_list

#Calling the list from the function 
merged_list = merge(keys_list, values_list)

#sorting by the values
merged_list = sorted(merged_list, key=lambda x:x[1])

print (merged_list)    

print ("\n")
print ("\n")
print ("\n")
print ("\n") 

