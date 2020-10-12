
from pyspark.sql import SparkSession
from pyspark import SparkConf
import os
from pyspark.sql.functions import col, unix_timestamp, when, max
from pyspark.sql import Window
from datetime import datetime
import logging

from pyspark.sql.types import *

now = datetime.now()
logging.basicConfig(filename="log/raw2csv-{}.log".format(now.strftime("%Y-%m-%d-%H-%M-%S")),
                    format='%(asctime)s %(message)s',
                    filemode='w')
logger = logging.getLogger()
# datetime object containing current date and time
logger.setLevel(logging.INFO)


def extract(input, spark):
    logger.info("start reading json file from " + input)
    df = spark.read.json(input)
    logger.info("finish reading json file")
    return df


def tranform(df):
    select_df = df.select(col("id").alias("tid"),
                          col("id_str").alias("tid_str"),
                          unix_timestamp(col("created_at"),
                                         "EEE MMM dd HH:mm:ss ZZZZ yyyy").alias("timestamp"),
                          col("text").alias("content"),
                          col("user.id").alias("sender_uid"),
                          col("user.id_str").alias("sender_uid_str"),
                          col("user.followers_count").alias("followers_count"),
                          col("favorite_count"),
                          col("retweet_count"),
                          col("lang"))
    logger.info("Created select_df")

    filter_df = select_df.filter(col("lang").isin(["en"]) &
                                 (col("tid").isNotNull() | col("tid_str").isNotNull()) &
                                 (col("sender_uid").isNotNull() | col("sender_uid_str").isNotNull()) &
                                 col("timestamp").isNotNull() &
                                 col("content").isNotNull())
    logger.info("Created filter_df")

    # combine uid and uid_str
    clean_df = filter_df.withColumn("tid", when(col("tid_str").isNotNull(), col("tid_str")).otherwise(col("tid"))). \
        withColumn("sender_uid", when(col("sender_uid_str").isNotNull(),
                                      col("sender_uid_str")).otherwise(col("sender_uid"))). \
        select("tid", "sender_uid", "content", "timestamp", "followers_count", "favorite_count", "retweet_count")
    query3_df = clean_df.withColumn("tid", clean_df["tid"].cast(LongType())). \
                        withColumn("sender_uid", clean_df["sender_uid"].cast(LongType())). \
                        withColumn("timestamp", clean_df["timestamp"].cast(LongType())). \
                        withColumn("followers_count", clean_df["followers_count"].cast(IntegerType())). \
                        withColumn("favorite_count", clean_df["favorite_count"].cast(IntegerType())). \
                        withColumn("retweet_count", clean_df["retweet_count"].cast(IntegerType()))

    logger.info("Created clean_df")
    return query3_df


def load2csv(query3_df, output):
    query3_df.write.mode("overwrite").csv(output, sep="⊢", header=False)
    logger.info("Saved tweet_df to csv files")


def load2sql(query3_df):
    # Load to SQL
    # jdbc configuration
    jdbcPort = "3306"
    jdbcDatabase = "all_db"
    jdbcUsername = "root"
    jdbcPassword = "password"
    jdbcHostname = "10.150.0.6"

    jdbcUrl = "jdbc:mysql://" + jdbcHostname + ":" + jdbcPort + "/" + jdbcDatabase + "?serverTimezone=UTC"

    # write tweet table
    logger.info("loading tweet_df to " + jdbcHostname + "/" + jdbcDatabase + "tweet_table")
    query3_df.write.format('jdbc').options(
        url=jdbcUrl,
        driver='com.mysql.jdbc.Driver',
        dbtable="query3_50",
        user=jdbcUsername,
        password=jdbcPassword).mode('overwrite').save()
    logger.info("finish loading tweet_df")


def read_csv(csv_input, spark):
    # Read from intermediate file
    query3_schema = StructType([
        StructField("tid", LongType(), False),
        StructField("sender_uid", LongType(), False),
        StructField("content", StringType(), False),
        StructField("timestamp", LongType(), False),
        StructField("followers_count", IntegerType(), False),
        StructField("favorite_count", IntegerType(), False),
        StructField("retweet_count", IntegerType(), False)]
    )
    logger.info("reading tweet_df from " + csv_input)
    query3_df = spark.read.csv(csv_input, sep="⊢", header=False, multiLine=True, schema=query3_schema)
    return query3_df

local = False
# get spark session
if local:
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("twitter-local-raw2csv") \
        .config("spark.some.config.option", "SparkSessionExample") \
        .getOrCreate()
else:
    config = SparkConf().setAll(
        [('spark.driver.extraClassPath', '/usr/share/java/mysql-connector-java-8.0.19.jar'),
         ('spark.jars', '/usr/share/java/mysql-connector-java-8.0.19.jar'),
         ('spark.driver.userClassPathFirst', True),
         ('spark.executor.userClassPathFirst', True)])
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("twitter-yarn-raw2csv") \
        .config(conf=config) \
        .getOrCreate()

input = "file:///Users/qiuchenzhang/Code/CMU/15619/Ying_Liu_Zhi_Zhu-S20/phase1/twitter/ETL/input/query2_ref.json"
output = "file:///Users/qiuchenzhang/Code/CMU/15619/Ying_Liu_Zhi_Zhu-S20/phase1/twitter/ETL/output/query3"
#
# input = "wasb://twitter@cmuccpublicdatasets.blob.core.windows.net/s20/part-r-000[0-4]?.gz"
output = "hdfs:///input/query3/"

# df = extract(input, spark)
# query3_df = tranform(df)
# load2csv(query3_df, output)

query3_df = read_csv(output, spark)
load2sql(query3_df)
