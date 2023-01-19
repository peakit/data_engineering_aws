#!/usr/bin/env python
# coding: utf-8

# # Data Wrangling with Spark SQL Quiz
# 
# This code uses the same dataset and most of the same questions from the earlier code using dataframes. For this scropt, however, use Spark SQL instead of Spark Data Frames.


from pyspark.sql import SparkSession

# TODOS: 
# 1) import any other libraries you might need
# 2) instantiate a Spark session 
# 3) read in the data set located at the path "data/sparkify_log_small.json"
# 4) create a view to use with your SQL queries
# 5) write code to answer the quiz questions 
spark = SparkSession.builder \
            .appName("Spark SQL Quiz") \
            .getOrCreate()
path = '/home/workspace/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/lesson-2-spark-essentials/exercises/data/sparkify_log_small.json'
user_log_df = spark.read.json(path)

user_log_df.createOrReplaceTempView("user_log_table")
# # Question 1
# 
# Which page did user id ""(empty string) NOT visit?

# TODO: write your code to answer question 1
spark.sql(
    '''
    SELECT DISTINCT page
      FROM user_log_table
    EXCEPT
    SELECT DISTINCT page
      FROM user_log_table
     WHERE userId = ""
    '''
).show()


# # Question 2 - Reflect
# 
# Why might you prefer to use SQL over data frames? Why might you prefer data frames over SQL?

# # Question 3
# 
# How many female users do we have in the data set?

# TODO: write your code to answer question 3
spark.sql(
    '''
    SELECT COUNT(DISTINCT userId)
      FROM user_log_table
     WHERE gender = 'F'
    '''
).show()

# # Question 4
# 
# How many songs were played from the most played artist?

# TODO: write your code to answer question 4
spark.sql(
    '''
    SELECT artist, COUNT(song)
     FROM user_log_table
    GROUP BY artist
    ORDER BY COUNT(song) DESC
    LIMIT 1
    '''
).show()

# # Question 5 (challenge)
# 
# How many songs do users listen to on average between visiting our home page? Please round your answer to the closest integer.

# TODO: write your code to answer question 5

