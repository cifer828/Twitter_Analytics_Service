from pyspark.sql import SparkSession
from pyspark import SparkConf
import os
from pyspark.sql.functions import col, max
from pyspark.sql import Window
from pyspark.sql.types import *
import logging
from datetime import datetime


class CSV2SQL:
    def __init__(self, local=True, choice=-1):
        self.local = local
        self.choice = choice
        # Choice
        # 1 single file
        # 0 all files
        # -1 test file
        # 50 for first 50 files
        prefix = "file:///Users/qiuchenzhang/Code/CMU/15619/Ying_Liu_Zhi_Zhu-S20/phase1/twitter/ETL/output/" if local else "hdfs:///output/"
        if choice == 1:
            self.output = prefix + "oneOutput/"
            self.table_prefix = "one_"
        elif choice == 0:
            self.output = prefix + "allOutput/"
            self.table_prefix = "all_"
        elif choice == -1:
            self.output = prefix + "testOutput/"
            self.table_prefix = "test_"
        elif choice == 50:
            self.output = prefix + "fiftyOutput/"
            self.table_prefix = "fifty_"

        # specify JAVA_HOME when running locally
        if self.local:
            os.environ["JAVA_HOME"] = "/usr/local/opt/jenv/versions/openjdk64-1.8.0.242"

        # get spark session
        if local:
            self.spark = SparkSession.builder \
                        .master("local[*]") \
                        .appName("twitter-local-csv2sql") \
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
                .appName("twitter-yarn-csv2sql") \
                .config(conf=config) \
                .getOrCreate()

        self.tweet_df = None
        self.user_df = None
        self.logger = None
        self.init_log()

    def init_log(self):
        # Create and configure logger
        now = datetime.now()
        logging.basicConfig(filename="log/csv2sql[{}]-{}.log".format(self.choice, now.strftime("%Y-%m-%d-%H-%M-%S")),
                            format='%(asctime)s %(message)s',
                            filemode='w')
        self.logger = logging.getLogger()
        # datetime object containing current date and time
        self.logger.setLevel(logging.INFO)

        if self.local:
            self.logger.info("\n*******************\ncreate spark session locally.")
        else:
            self.logger.info("\n*******************\ncreate spark session on server.")

    def read_tweet_df(self):
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

    def load2sql(self, db_local=False):
        # Load to SQL
        # jdbc configuration
        jdbcPort = "3306"
        jdbcDatabase = ""
        jdbcUsername = ""
        jdbcPassword = ""
        jdbcHostname = "localhost" if db_local else "10.142.0.14"

        jdbcUrl = "jdbc:mysql://" + jdbcHostname + ":" + jdbcPort + "/" + jdbcDatabase + "?serverTimezone=UTC"

        # write tweet table
        self.logger.info(
            "loading tweet_df to " + jdbcHostname + "/" + jdbcDatabase + " as " + self.table_prefix + "tweet_table")
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

    def read_user(self):
        user_df_file = self.output + "userDf/*.csv"
        user_schema = StructType([
            StructField("uid", LongType(), False),
            StructField("screen_name", StringType(), False),
            StructField("description", StringType(), False),
            StructField("timestamp", LongType(), False)]
        )
        self.logger.info("reading user_df from " + user_df_file)
        self.user_df = self.spark.read.csv(user_df_file, sep="⊢", header=True, multiLine=True, schema=user_schema)
        self.logger.info("finish reading tweet_df: " + str(self.user_df.count()) + " items")

    def read_and_clean_user_mid(self):
        # remove duplicate users
        user_mid_df_file = self.output + "userMidDf/*.csv"
        self.logger.info("reading user_mid_df from " + user_mid_df_file)
        user_mid_df = self.spark.read.csv(user_mid_df_file, sep="⊢", header=True, multiLine=True, inferSchema=True)
        self.logger.info("Get {} items from user_mid_df".format(user_mid_df.count()))
        self.logger.info("Get the latest information of users from user_mid_df.")
        w = Window.partitionBy(col("uid"))
        self.user_df = user_mid_df.withColumn("latestTime", max("timestamp").over(w)). \
            filter(col("latestTime") == col("timestamp")). \
            drop("timestamp"). \
            withColumnRenamed("latestTime", "timestamp"). \
            dropDuplicates(["uid"])
        self.logger.info("Created user_df: get {} items.".format(self.user_df.count()))


if __name__ == "__main__":
    # local: run locally or remotely
    # choice: 1 single file, 0 all files, -1 test file, 50 for first 50 files
    csv2sql = CSV2SQL(local=True, choice=1)  # local, test
    # csv2sql.read_and_clean_user_mid()
    csv2sql.read_user()
    csv2sql.read_tweet_df()
    csv2sql.load2sql(db_local=True)
