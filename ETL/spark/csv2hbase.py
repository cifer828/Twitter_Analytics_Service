#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: Cifer Z.
date: 3/21/20
"""

from pyspark.sql import SparkSession
from pyspark import SparkConf
import os
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
from datetime import datetime


class CSV2HBASE:
    def __init__(self, local=True, choice=-1):
        self.local = local
        self.choice = choice
        # Choice
        # 1 single file
        # 0 all files
        # -1 test file
        prefix = "file:///Users/qiuchenzhang/Code/CMU/15619/Ying_Liu_Zhi_Zhu-S20/phase1/twitter/ETL/output/" if local else "hdfs:///output/"
        self.output = prefix + "oneOutput/" if choice == 1 else prefix + "allOutput/" if choice == 0 else prefix + "testOutput/"
        self.table_prefix = "one_" if choice == 1 else "all_" if choice == 0 else "test_"

        # specify JAVA_HOME when running locally
        if self.local:
            os.environ["JAVA_HOME"] = "/usr/local/opt/jenv/versions/openjdk64-1.8.0.242"

        # get spark session
        if local:
            self.spark = SparkSession.builder \
                        .master("local[*]") \
                        .appName("twitter-local-csv2hbase") \
                        .config("spark.some.config.option", "SparkSessionExample") \
                        .getOrCreate()
        else:
            config = SparkConf().setAll(
                [('spark.driver.extraClassPath', '/home/qiuchenzhang/mysql-connector-java-8.0.19.jar'),
                 ('spark.jars', '/home/qiuchenzhang/mysql-connector-java-8.0.19.jar'),
                 ('spark.driver.userClassPathFirst', True),
                 ('spark.executor.userClassPathFirst', True)])
            self.spark = SparkSession.builder \
                .master("yarn") \
                .appName("twitter-yarn-csv2hbase") \
                .config(conf=config) \
                .getOrCreate()

        self.tweet_df = None
        self.user_df = None
        self.logger = None
        self.init_log()

    def init_log(self):
        # Create and configure logger
        now = datetime.now()
        logging.basicConfig(filename="log/csv2hbase-" + now.strftime("%Y-%m-%d-%H-%M-%S") +".log",
                            format='%(asctime)s %(message)s',
                            filemode='w')
        self.logger = logging.getLogger()
        # datetime object containing current date and time
        self.logger.setLevel(logging.INFO)

        if self.local:
            self.logger.info("\n*******************\ncreate spark session locally.")
        else:
            self.logger.info("\n*******************\ncreate spark session on server.")

    def read_df(self):
        # Read from intermediate file
        tweet_df_file = self.output + "tweetDf/*.csv"
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
        self.logger.info("reading tweet_df from " + tweet_df_file)
        self.tweet_df = self.spark.read.csv(tweet_df_file, sep="⊢", header=True, multiLine=True, schema=tweet_schema)

        self.logger.info("finish reading tweet_df: " + str(self.tweet_df.count()) + " items")

        user_df_file = self.output + "userDf/*.csv"
        user_schema = StructType([
            StructField("uid", LongType(), False),
            StructField("screen_name", StringType(), False),
            StructField("description", StringType(), False),
            StructField("timestamp", LongType(), False)]
        )
        self.logger.info("reading user_df from " + user_df_file)
        self.user_df = self.spark.read.csv(user_df_file, sep="⊢", header=True, multiLine=True, schema=user_schema)
        self.logger.info("finish reading tweet_df: " + str(self.tweet_df.count()) + " items")

    def load2hbase(self, hbase_local=False):
        # Load to SQL
        # jdbc configuration
        jdbcPort = "3306"
        jdbcDatabase = ""
        jdbcUsername = ""
        jdbcPassword = ""
        jdbcHostname = "localhost" if hbase_local else "35.243.231.47"

        jdbcUrl = "jdbc:mysql://" + jdbcHostname + ":" + jdbcPort + "/" + jdbcDatabase + "?serverTimezone=UTC"

        # write tweet table
        self.logger.info(
            "loading tweet_df to " + jdbcHostname + "/" + jdbcDatabase + self.table_prefix + "tweet_table")
        self.tweet_df.write.format('jdbc').options(
            url=jdbcUrl,
            driver='com.mysql.cj.jdbc.Driver',
            dbtable=self.table_prefix + "tweet_table",
            user=jdbcUsername,
            password=jdbcPassword).mode('overwrite').save()
        self.logger.info("finish loading tweet_df")

        # write user table
        self.logger.info(
            "loading user_df to " + jdbcHostname + "/" + jdbcDatabase + " as " + self.table_prefix + "user_table")
        self.user_df.write.format('jdbc').options(
            url=jdbcUrl,
            driver='com.mysql.cj.jdbc.Driver',
            dbtable=self.table_prefix + "user_table",
            user=jdbcUsername,
            password=jdbcPassword).mode('overwrite').save()
        self.logger.info("finish loading user_df")