from pyspark.sql import SparkSession
from pyspark import SparkConf
import os
from pyspark.sql.functions import col, unix_timestamp, concat_ws, when, length, max
from pyspark.sql import Window
from datetime import datetime
import logging
from csv2sql import CSV2SQL


class RAW2CSV:
    def __init__(self, local=True, choice=-1, convert_user=False):
        self.local = local
        self.choice = choice
        self.convert_user = convert_user
        # Choice
        # 1 single file
        # 0 all files
        # -1 test file
        # 50 fifty file
        prefix = "file:///Users/qiuchenzhang/Code/CMU/15619/Ying_Liu_Zhi_Zhu-S20/phase1/twitter/ETL/output/" if local else "hdfs:///output/"

        local_input_prefix = "file:///Users/qiuchenzhang/Code/CMU/15619/Ying_Liu_Zhi_Zhu-S20/phase1/twitter/ETL/input/"
        test_file = local_input_prefix + "query2_ref.json" if local else "hdfs:///input/query2_ref.txt"
        one_file = local_input_prefix + "part-r-00000.gz" if local else "gs://cmuccpublicdatasets/twitter/s20/part-r-00000.gz"
        all_files = "gs://cmuccpublicdatasets/twitter/s20/*.gz"
        fifty_files = "gs://cmuccpublicdatasets/twitter/s20/part-r-000[0-4]?.gz"

        if choice == 1:
            self.output = prefix + "oneOutput/"
            self.table_prefix = "one_"
            self.input = one_file
        elif choice == 0:
            self.output = prefix + "allOutput/"
            self.table_prefix = "all_"
            self.input = all_files
        elif choice == -1:
            self.output = prefix + "testOutput/"
            self.table_prefix = "test_"
            self.input = test_file
        elif choice == 50:
            self.output = prefix + "fiftyOutput/"
            self.table_prefix = "fifty_"
            self.input = fifty_files

        # specify JAVA_HOME when running locally
        if self.local:
            os.environ["JAVA_HOME"] = "/usr/local/opt/jenv/versions/openjdk64-1.8.0.242"

        # get spark session
        if local:
            self.spark = SparkSession.builder \
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
            self.spark = SparkSession.builder \
                .master("yarn") \
                .appName("twitter-yarn-raw2csv") \
                .config(conf=config) \
                .getOrCreate()

        self.df = None
        self.select_df = None
        self.filter_df = None
        self.user_mid_df = None
        self.tweet_df = None
        self.user_df = None
        self.logger = None
        self.init_log()

    def init_log(self):
        # Create and configure logger
        now = datetime.now()
        logging.basicConfig(filename="log/raw2csv[{}]-{}.log".format(self.choice, now.strftime("%Y-%m-%d-%H-%M-%S")),
                            format='%(asctime)s %(message)s',
                            filemode='w')
        self.logger = logging.getLogger()
        # datetime object containing current date and time
        self.logger.setLevel(logging.INFO)

        if self.local:
            self.logger.info("\n*******************\ncreate spark session locally.")
        else:
            self.logger.info("\n*******************\ncreate spark session on server.")

    def extract(self):
        self.logger.info("start reading json file from " + self.input)
        self.df = self.spark.read.json(self.input)
        self.logger.info("finish reading json file")

    def tranform(self):
        # Transform
        # 1. convert time to timetsamp
        # 2. concatenate hashtags with ,
        self.select_df = self.df.select(col("id").alias("tid"),
                                        col("id_str").alias("tid_str"),
                                        unix_timestamp(col("created_at"),
                                                       "EEE MMM dd HH:mm:ss ZZZZ yyyy").alias("timestamp"),
                                        col("text").alias("content"),
                                        col("in_reply_to_user_id").alias("reply_to_uid"),
                                        col("in_reply_to_user_id_str").alias("reply_to_uid_str"),
                                        col("user.id").alias("sender_uid"),
                                        col("user.id_str").alias("sender_uid_str"),
                                        col("user.screen_name").alias("sender_screen_name"),
                                        col("user.description").alias("sender_description"),
                                        col("retweeted_status.user.id").alias("retweet_to_uid"),
                                        col("retweeted_status.user.id_str").alias("retweet_to_uid_str"),
                                        col("retweeted_status.user.screen_name").alias("retweet_to_uid_screen_name"),
                                        col("retweeted_status.user.description").alias("retweet_to_uid_description"),
                                        concat_ws(",", col("entities.hashtags.text")).alias("hashtags"),
                                        col("lang"))
        self.logger.info(
            "Created select_df: 1. select useful fields 2. convert time to timetsamp 3. concatenate hashtags with")

        # filter out malformed tweets and tweets not using specific languages
        # 1. Cannot be parsed).alias(a JSON object
        # 2. Both id and id_str of the tweet object are missing or null
        # 3. Both id and id_str of the user object are missing or null
        # 4. created_at is missing or null
        # 5. text is missing or null or empty_string
        # 6. hashtag array missing or null or of length zero/empty
        self.filter_df = self.select_df.filter(col("lang").isin(["ar", "en", "fr", "in", "pt", "es", "tr", "ja"]) &
                                               (col("tid").isNotNull() | col("tid_str").isNotNull()) &
                                               (col("sender_uid").isNotNull() | col("sender_uid_str").isNotNull()) &
                                               col("timestamp").isNotNull() &
                                               col("content").isNotNull() &
                                               col("hashtags").isNotNull()).where(length(col("hashtags")) > 0)
        self.logger.info(
            "Created filter_df: filter out malformed tweets and tweets not using specific languages")

        # combine uid and uid_str
        clean_df = self.filter_df.withColumn("tid",
                                             when(col("tid_str").isNotNull(), col("tid_str")).otherwise(col("tid"))). \
            withColumn("sender_uid",
                       when(col("sender_uid_str").isNotNull(),
                            col("sender_uid_str")).otherwise(col("sender_uid"))). \
            withColumn("reply_to_uid",
                       when(col("reply_to_uid_str").isNotNull(),
                            col("reply_to_uid_str")).otherwise(col("reply_to_uid"))). \
            withColumn("retweet_to_uid",
                       when(col("retweet_to_uid_str").isNotNull(),
                            col("retweet_to_uid_str")).otherwise(col("retweet_to_uid"))). \
            drop("tid_str", "sender_uid_str", "reply_to_uid_str", "retweet_to_uid_str")
        self.logger.info(
            "Created clean_df: combine uid and uid_str")

        # tweetDf is for tweet_table
        self.tweet_df = clean_df.drop("sender_screen_name", "sender_description", "retweet_to_uid_screen_name",
                                      "retweet_to_uid_description")
        # userDf is for user_table
        senderDf = clean_df.select("sender_uid", "sender_screen_name", "sender_description", "timestamp")

        self.user_mid_df = clean_df.select(col("retweet_to_uid").alias("uid"),
                                           col("retweet_to_uid_screen_name").alias("screen_name"),
                                           col("retweet_to_uid_description").alias("description"),
                                           col("timestamp")). \
            filter(col("uid").isNotNull()). \
            union(senderDf)
        self.logger.info(
            "Created tweet_df and user_mid_df")

        # find the latest information of each users
        # may cost a lot of time
        # only execute when running one file
        if self.convert_user:
            w = Window.partitionBy(col("uid"))
            self.user_df = self.user_mid_df.withColumn("latestTime", max("timestamp").over(w)). \
                filter(col("latestTime") == col("timestamp")). \
                drop("timestamp"). \
                withColumnRenamed("latestTime", "timestamp"). \
                dropDuplicates(["uid"])
            self.logger.info(
                "Created user_df: find the latest information of each users")

    def load2csv(self):
        # Load
        # Load to intermediate file
        # save to intermediate file, which dilimiter should use ???
        self.tweet_df.write.mode("overwrite").csv(self.output + "tweetDf", sep="⊢", header=True)  # overwrite old result
        self.logger.info("Saved tweet_df to csv files")

        if self.convert_user:
            self.user_df.write.mode("overwrite").csv(self.output + "userDf", sep="⊢",
                                                     header=True)  # overwrite old result
            self.logger.info("Saved user_df to csv files")

        # self.select_df.write.mode("overwrite").csv(self.output + "selectDf", sep="⊢",
        #                                            header=True)  # overwrite old result
        # self.logger.info("Saved select_df to csv files")
        # self.filter_df.write.mode("overwrite").csv(self.output + "filterDf", sep="⊢",
        #                                            header=True)  # overwrite old result
        # self.logger.info("Saved filter_df to csv files")
        # self.user_mid_df.write.mode("overwrite").csv(self.output + "userMidDf", sep="⊢",
        #                                              header=True)  # overwrite old result
        # self.logger.info("Saved user_mid_df to csv files")

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
            "loading tweet_df to " + jdbcHostname + "/" + jdbcDatabase + self.table_prefix + "tweet_table")
        self.tweet_df.write.format('jdbc').options(
            url=jdbcUrl,
            driver='com.mysql.jdbc.Driver',
            dbtable=self.table_prefix + "tweet_table",
            user=jdbcUsername,
            password=jdbcPassword).mode('overwrite').save()
        self.logger.info("finish loading tweet_df")

        if self.convert_user:
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


if __name__ == "__main__":
    # json -> csv
    # local: run locally or remotely
    # choice: 1 single file, 0 all files, -1 test file, 50 for first 50 files
    my_choice = -1
    raw2csv = RAW2CSV(local=True, choice=my_choice, convert_user=True)  # yarn, one file
    raw2csv.extract()
    raw2csv.tranform()
    raw2csv.load2csv()
    raw2csv.load2sql(db_local=True)