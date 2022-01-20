# -*- coding: utf-8 -*-
"""
author SparkByExamples.com
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.master("local[*]") \
    .appName('CarSalesDemo') \
    .getOrCreate()

print("APP Name :" + spark.sparkContext.appName);
print("Master :" + spark.sparkContext.master);

readDf = spark.read.option("header", True).csv(
    "/Users/bibinnahas/PycharmProjects/CarSalesDemo/resources/transactions.csv")
modifiedDf = readDf.withColumn('dateInYYYY-MM-DD', col('purchase_timestamp').substr(1, 10)).drop('purchase_timestamp')

PagesPerDayDf = modifiedDf.groupby(
    'dateInYYYY-MM-DD').count().orderBy('dateInYYYY-MM-DD')

PurchaseByDayDf = modifiedDf.groupby(
    'dateInYYYY-MM-DD').count().orderBy('dateInYYYY-MM-DD')

modifiedDf.show()
PagesPerDayDf.show()

# df.withColumn('year', col('purchase_timestamp').substr(1, 10)).drop('purchase_timestamp').filter(col("year").contains("2019-12-24")).show()
