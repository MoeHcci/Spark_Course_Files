#Example 4: Find the maximum temp observed for each weather station for the year 1800
'''
The results should look like this:
('ITE00100554', -14.8)
('EZE00100082', -13.5)
The above is for min
You must type the code your own way not using the course's way
'''
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MaxTemp1800")

sc = SparkContext(conf = conf)

rdd = sc.textFile ("/home/mohamd/Documents/web_dev/Spark_Python/1800.csv")
# print (rdd.take(10)) # Prints the first 10 lines of the rdd
def parseLine (x):
    fields = x.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = fields[3]
    return (stationID, entryType,temperature )

#The below convers the dictionary to an RDD. Here the map applies the function parseLine to every element in the lines rdd
rdd = rdd.map(parseLine)
    # Notes about map:
    #     Return a new RDD by applying a function to each element of this RDD.
    #     the output of map transformations would always have the same number of records as input.

#The below does mathematical calculations that involve the usage of filter. It will only return the rows that include 'TMIN' in x[1]
maxTemps = rdd.filter(lambda x: 'TMAX' in x[1])
    # Notes about filter
    #     Return a new RDD containing only the elements that satisfy a predicate.
stationTemps = maxTemps.map(lambda x: (x[0], x[2]))

maxTemps = stationTemps.reduceByKey(lambda x, y: max(x, y))
# Notes about reduceByKey
#     Merge the values for each key using an associative and commutative reduce function.

#We must use .collect() to be able to return the data or print it as written below
results = maxTemps.collect()
# Notes about collect
#     Return a list that contains all the elements in this RDD.
print ("\n")
for x in results:
    print (x)
print ("\n")
