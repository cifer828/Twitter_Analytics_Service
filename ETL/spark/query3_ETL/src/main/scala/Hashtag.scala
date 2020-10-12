import java.sql.DriverManager
import java.util.{Locale, Properties}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scala.math.{max, min}
import org.apache.spark.sql._


object Hashtag {

  def raw2df(inputUrl: String, spark: SparkSession): DataFrame = {
    import spark.implicits._
    val df = spark.read.json(inputUrl)
    val select_df = df.select(col("id").alias("tid"),
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

    val filter_df = select_df.filter(col("lang").isin(("en")) &&
      (col("tid").isNotNull || col("tid_str").isNotNull) &&
      (col("sender_uid").isNotNull || col("sender_uid_str").isNotNull) &&
      col("timestamp").isNotNull &&
      col("content").isNotNull)

    val query3_df = filter_df.withColumn("tid", when(col("tid_str").isNotNull, col("tid_str")).otherwise(col("tid"))).
      withColumn("sender_uid", when(col("sender_uid_str").isNotNull,
        col("sender_uid_str")).otherwise(col("sender_uid"))).
      select("tid", "sender_uid", "content", "timestamp", "followers_count", "favorite_count", "retweet_count")
    query3_df
  }

  def writeCSV(query3_df: DataFrame, output: String): Unit ={
    query3_df.write.
      mode("overwrite").
      option("encoding", "utf8").
      option("sep", "⊢").
      option("multiLine", "true").
      option("header", "false").
      csv(output)
  }

  def readCSV(input_url: String, spark: SparkSession): DataFrame = {
    val tweet_schema = StructType(
      Array(
        StructField("tid", LongType, false),
        StructField("sender_uid", LongType, false),
        StructField("content", StringType, false),
        StructField("timestamp", LongType, true),
        StructField("followers_count", IntegerType, false),
        StructField("favorite_count", IntegerType, true),
        StructField("retweet_count", IntegerType, false)
      )
    )
    val tweet_df = spark.read.schema(tweet_schema).
      option("encoding", "utf8").
      option("sep", "⊢").
      option("multiLine", "true").
      option("header", "false").
      csv(input_url)
    tweet_df

  }

  def loadToMySQL(reduce_hashtag_df: DataFrame): Unit = {
    // Load to MySQL
    // set credentials
    val jdbcPort = "3306"
    val jdbcDatabase = ""
    val jdbcUsername = ""
    val jdbcPassword = "" // the password you set
    val jdbcHostname = "13.82.133.194" // the external IP address of you MySQL DB

    val jdbcUrl = s"jdbc:mysql://$jdbcHostname:$jdbcPort/$jdbcDatabase" + "?serverTimezone=UTC"

    val driverClass = "com.mysql.jdbc.Driver"
    Class.forName(driverClass) // check jdbc driver

    // set connection properties
    val connectionProperties = new Properties()
    connectionProperties.put("user", s"$jdbcUsername")
    connectionProperties.put("password", s"$jdbcPassword")
    connectionProperties.setProperty("Driver", driverClass)
    connectionProperties.setProperty("useServerPrepStmts", "false") // take note of this configuration, and understand what it does
    connectionProperties.setProperty("rewriteBatchedStatements", "true") // take note of this configuration, and understand what it does

    // first drop the table if it already exists
    val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
    assert(!connection.isClosed)
    //    val stmt = connection.createStatement()
    //    stmt.executeUpdate("drop table if exists all_clean_hashtag")

    // write the dataframe to a table called "test_twitter"
    reduce_hashtag_df.write.
      mode("overwrite").
      option("encoding", "utf8").
      //      option("createTableColumnTypes", "sender_uid BIGINT, hashtags LONGVARCHAR").
      jdbc(jdbcUrl, "query3", connectionProperties)
  }

  /**
   * @param args it should be called with two arguments, the input path, and the output path.
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.sparkSession()
    //    val input = "file:///Users/qiuchenzhang/Code/CMU/15619/Ying_Liu_Zhi_Zhu-S20/phase1/twitter/ETL/input/query2_ref.json"
    //    val output = "file:///Users/qiuchenzhang/Code/CMU/15619/Ying_Liu_Zhi_Zhu-S20/phase1/twitter/ETL/output/query3"

    val input = "wasb://twitter@cmuccpublicdatasets.blob.core.windows.net/s20/part-r-000[0-5]?.gz"
    val output = "wasb:///query3_output_50"

    val query3_df = raw2df(input, spark)
    writeCSV(query3_df, output)
    //    val query3_df = readCSV(output, spark)
    loadToMySQL(query3_df)
  }
}
