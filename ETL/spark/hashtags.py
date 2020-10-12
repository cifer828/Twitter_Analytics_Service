#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: Cifer Z.
date: 4/8/20
"""

from pyspark.sql import SparkSession
from pyspark import SparkConf
import os
from pyspark.sql.functions import col, max
from pyspark.sql import Window
from pyspark.sql.types import *
import logging
from datetime import datetime


local = True

if local:
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("twitter-local-raw2csv") \
        .config("spark.some.config.option", "SparkSessionExample") \
        .getOrCreate()
else:
    config = SparkConf().setAll(
        [('spark.driver.extraClassPath', '/home/qiuchenzhang/mysql-connector-java-8.0.19.jar'),
         ('spark.jars', '/home/qiuchenzhang/mysql-connector-java-8.0.19.jar'),
         ('spark.driver.userClassPathFirst', True),
         ('spark.executor.userClassPathFirst', True)])
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("twitter-yarn-raw2csv") \
        .config(conf=config) \
        .getOrCreate()

# tweet_df_file = "s3://twitter-yingliuzhizhu/tweetDf"

tweet_df_file = "file:///Users/qiuchenzhang/Code/CMU/15619/Ying_Liu_Zhi_Zhu-S20/phase1/twitter/ETL/output/testOutput/tweetDf"
tweet_schema = StructType([
    StructField("tid", LongType(), False),
    StructField("timestamp", LongType(), False),
    StructField("content", StringType(), False),
    StructField("reply_to_uid", LongType(), True),
    StructField("sender_uid", LongType(), False),
    StructField("retweet_to_uid", LongType(), True),
    StructField("hashtags", StringType(), False),
    StructField("lang", StringType(), False)]
)
tweet_df = spark.read.csv(tweet_df_file, sep="⊢", header=True, multiLine=True, schema=tweet_schema)

hashtag_df = tweet_df.select(col("sender_uid"),
                             col("hashtags"))


hashtag_rdd = hashtag_df.rdd.map(tuple)
hashtag_rdd.map(hashtag_rdd)
hashtag_rdd.foreach(print)


