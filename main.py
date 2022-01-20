# -*- coding: utf-8 -*-
"""
author SparkByExamples.com
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, col, lit, from_unixtime, unix_timestamp

spark = SparkSession.builder.master("local[*]") \
    .appName('CarSalesDemo') \
    .getOrCreate()

print("APP Name :" + spark.sparkContext.appName);
print("Master :" + spark.sparkContext.master);

# Weblog Schema
# schema = schema = StructType([
#     StructField("ip", StringType(), True),
#     StructField("dash", StringType(), True),
#     StructField("username", StringType(), True),
#     StructField("timestamp", StringType(), True),
#     StructField("some_count1", StringType(), True),
#     StructField("action", StringType(), True),
#     StructField("some_count2", StringType(), True),
#     StructField("some_count3", StringType(), True),
#     StructField("url", StringType(), True),
#     StructField("device", StringType(), True)]);

# Read Input files
weblogDf = spark.read.csv('/Users/bibinnahas/PycharmProjects/CarSalesDemo/resources/weblog.txt', inferSchema=True,
                          header=False, sep=' ', ignoreLeadingWhiteSpace=True)
transactionsDf = spark.read.option("header", True).csv(
    "/Users/bibinnahas/PycharmProjects/CarSalesDemo/resources/transactions.csv")

# Modify Input files
modifiedTransactionsDf = transactionsDf.withColumn('dateInYYYY-MM-DD', col('purchase_timestamp').substr(1, 10)).drop(
    'purchase_timestamp')
modifiedWeblogDf = weblogDf \
    .withColumnRenamed("_c0", "ip") \
    .drop("_c1") \
    .withColumnRenamed("_c2", "username") \
    .withColumnRenamed("_c3", "timestamp") \
    .withColumn('year', col('timestamp').substr(9, 4)) \
    .withColumn('month', col('timestamp').substr(5, 3)) \
    .withColumn('day', col('timestamp').substr(2, 2)) \
    .drop("_c4") \
    .withColumnRenamed("_c5", "action") \
    .withColumnRenamed("_c6", "some_count1") \
    .withColumnRenamed("_c7", "some_count2") \
    .withColumnRenamed("_c8", "url") \
    .withColumnRenamed("_c9", "device") \
    .withColumn("month", from_unixtime(unix_timestamp(col("Month"), 'MMM'), 'MM')) \
    .select('ip', 'username',
            concat(col("year"), lit("-"), col("month"), lit("-"), col("day")),
            'action', 'some_count1', 'some_count2', 'url',
            'device').withColumnRenamed("concat(year, -, month, -, day)", "hit_date")
# Make into report formats
PurchaseByDayDf = modifiedTransactionsDf.groupby(
    'dateInYYYY-MM-DD').count().orderBy('dateInYYYY-MM-DD')

PurchaseByDayDf.show()
modifiedWeblogDf.show(10, False)
