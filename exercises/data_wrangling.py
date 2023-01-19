# Take care of any imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import sum, desc
from pyspark.sql import Window

from datetime import datetime

# Create the Spark Context
spark = SparkSession \
    .builder \
    .appName("Wrangling Data") \
    .getOrCreate()


# Complete the script
path = "/home/workspace/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/lesson-2-spark-essentials/exercises/data/sparkify_log_small.json"


# # Data Exploration 
# 
# # Explore the data set.
user_log_df = spark.read.json(path)

# View 5 records 
user_log_df.take(5)


# Print the schema
user_log_df.printSchema()


# Describe the dataframe
user_log_df.describe().show()


# Describe the statistics for the song length column
user_log_df.describe("length").show()


# Count the rows in the dataframe
print(user_log_df.count())
   

# Select the page column, drop the duplicates, and sort by page
user_log_df.select("page").dropDuplicates().sort("page").show()

# Select data for all pages where userId is 1046
user_log_df.where(user_log_df.userId == 1046).show()

# # Calculate Statistics by Hour
get_hour = udf(lambda x: datetime.fromtimestamp(x).hour)
user_log_df = user_log_df.withColumn("hour", get_hour(user_log_df.ts/1000))

# Count of songs in an hour
user_log_df.filter(user_log_df.page == "NextSong").groupBy(user_log_df.hour).count().orderBy(user_log_df.hour.cast("int")).show()

# Select just the NextSong page
user_log_df.filter(user_log_df.page == "NextSong").show()

# # Drop Rows with Missing Values
user_log_valid = user_log_df.dropna(how = "any", subset= ["userId", "sessionId"])

# How many are there now that we dropped rows with null userId or sessionId?
print(user_log_valid.count())

# select all unique user ids into a dataframe
user_log_valid.select("userId").dropDuplicates().sort(user_log_valid.userId.cast("int")).show()

# Select only data for where the userId column isn't an empty string (different from null)
user_log_valid.filter(user_log_valid.userId != "").show()

# # Users Downgrade Their Accounts
# 
# Find when users downgrade their accounts and then show those log entries. 
user_log_valid.filter(user_log_valid.page == "Submit Downgrade").show()


# Create a user defined function to return a 1 if the record contains a downgrade
is_downgrade = udf(lambda x: 1 if x == "Submit Downgrade" else 0, IntegerType())
user_log_valid = user_log_valid.withColumn("downgraded", is_downgrade("page"))

# Select data including the user defined function
user_log_valid.head()


# Partition by user id
# Then use a window function and cumulative sum to distinguish each user's data as either pre or post downgrade events.
windowVal = Window.partitionBy("userId").orderBy(desc("ts")).rangeBetween(Window.unboundedPreceding, 0)


# Fsum is a cumulative sum over a window - in this case a window showing all events for a user
# Add a column called phase, 0 if the user hasn't downgraded yet, 1 if they have
user_log_valid = user_log_valid.withColumn("phase", sum("downgraded").over(windowVal) )


# Show the phases for user 1138 
user_log_valid.show()
user_log_valid.select(
            ["userId", "firstname", "ts", "page", "level", "phase"]
        ).where(
            user_log_valid.userId == "1138"
        ).sort("ts").show()