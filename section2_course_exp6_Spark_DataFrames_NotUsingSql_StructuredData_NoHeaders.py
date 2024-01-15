# Find the total amount spent per customer

from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql.functions import round, col
import pyspark.sql.functions as F
from pyspark.sql.functions import sum

# 1. create the spark Session or activate an existing one
    # getorCreate() creates a SparkSession connects an already existing spark session
spark = SparkSession.builder.appName('CustomerOrders').getOrCreate()
    # the appName can be anything -->  Sets a name for the application, which will be shown in the Spark web UI.

# 2. Read the csv file and turn it into a data frame. If the headers do not have names they will be called _c#
df = spark.read.csv('customer-orders.csv', header=False)

# The columns have no headers. Therefore, add headers to them
df = df.toDF('customer', 'id', 'spending')

# Check the types of columns
df.printSchema()

# The spending/customer columns is not a numerical column therefore, convert it to numerical so we can apply analysis on it
df = df.withColumn( "spending",col("spending").cast("int"))
df = df.withColumn( "customer",col("customer").cast("int"))

# Check the types of columns
df.printSchema()


'''
The total spent analysis per customer
'''
# Group by the customer station and sort by the new total spent column
df = df.groupby('customer').agg(sum('spending')\
    .alias('total_spent'))
# 1. Grouping by the rows of the 'customer' column
# 2. Using agg to get the sum of 'spending' column per 'customer'
# 3. using '\' prior to .alias to create a new column name
    # Note: the .alias is enclosed within .agg

# sort by the new 'total_spent' column
df = df.sort(df.total_spent.desc())

# show the first 45 people
df.show(10)

# 4. The Spark session must be stopped so the next time this script runs it again getOrCreate a spark session
spark.stop() # We need this at the end of a spark script to ensure we stop it