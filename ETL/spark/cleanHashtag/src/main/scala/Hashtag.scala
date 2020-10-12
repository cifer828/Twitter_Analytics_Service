import java.sql.DriverManager
import java.util.{Locale, Properties}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.math.{max, min}


object Hashtag {

  def cleanHashtag(input_url: String, spark: SparkSession): RDD[(Long, String)] = {
    val tweet_schema = StructType(
      Array(
        StructField("tid", LongType, false),
        StructField("timestamp", LongType, false),
        StructField("content", StringType, false),
        StructField("reply_to_uid", LongType, true),
        StructField("sender_uid", LongType, false),
        StructField("retweet_to_uid", LongType, true),
        StructField("hashtags", StringType, false),
        StructField("lang", StringType, false)
      )
    )
    val tweet_df = spark.read.schema(tweet_schema).
      option("sep", "⊢").
      option("multiLine", "true").
      option("header", "true").
      csv(input_url)

    val hashtag_df = tweet_df.select("sender_uid", "hashtags")

    val hashtag_rdd = hashtag_df.rdd.map {
      case Row(sender_uid: Long, hashtags: String) =>
        (sender_uid, hashtags)
    }

    hashtag_rdd

  }

  def loadToMySQL(reduce_hashtag_df: DataFrame): Unit = {
    // Load to MySQL
    // set credentials
    val jdbcPort = "3306"
    val jdbcDatabase = ""
    val jdbcUsername = ""
    val jdbcPassword = "" // the password you set
    val jdbcHostname = "40.71.3.41" // the external IP address of you MySQL DB

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
      option("encoding", "UTF-8").
      mode("append").
      //      option("createTableColumnTypes", "sender_uid BIGINT, hashtags LONGVARCHAR").
      jdbc(jdbcUrl, "all_clean_hashtag", connectionProperties)
  }

  def concat(hashtag_rdd: RDD[(Long, String)], spark: SparkSession, popular_set: Set[String]): DataFrame = {
    val clean_hashtag_rdd = hashtag_rdd.flatMap {
      case (sender, hashtags) =>
        val ht = hashtags.split(",")

        var filterHt: List[(Long, String)] = List()
        for (i <- ht) {
          val lower = i.toLowerCase(Locale.ENGLISH)
          if (!popular_set.contains(lower)) {
            filterHt = List((sender, lower)) ::: filterHt
          }
        }
        filterHt
    }

    val reduce_hashtag_rdd = clean_hashtag_rdd.reduceByKey(_ + "," + _)
    import spark.implicits._
    val reduce_hashtag_df = reduce_hashtag_rdd.toDF("sender_uid", "hashtags")
    reduce_hashtag_df
  }

  def count(hashtagRdd: RDD[(Long, String)], spark: SparkSession, popular_set: Set[String]): DataFrame = {
    val invertRdd = hashtagRdd.flatMap {
      case (sender, hashtags) =>
        val ht = hashtags.split(",")

        var filterHt: List[((String, Long), Int)] = List()
        for (i <- ht) {
          val lower = i.toLowerCase(Locale.ENGLISH)
          if (!popular_set.contains(lower)) {
            filterHt = ((lower, sender), 1) :: filterHt
          }
        }
        filterHt
    }.reduceByKey(_ + _)

    val combineRdd = invertRdd.map { case ((h, k), cnt) => (h, (k, cnt)) }.groupByKey()

    val pairRdd = combineRdd.flatMap {
      case (_, combine) =>
        val list = combine.toList
        var ret: List[((Long, Long), Int)] = List()
        for (i <- 0 to list.size - 1) {
          val li = list(i)
          for (j <- i + 1 to list.size - 1) {
            val lj = list(j)
            ret = ((lj._1, li._1), li._2 + lj._2) :: ret
          }
        }
        ret
    }.map {
      case ((l1, l2), v) => (min(l1, l2).toString + "_" + max(l1, l2).toString, v)
    }.reduceByKey(_ + _)
    import spark.implicits._
    val reduce_hashtag_df = pairRdd.toDF("pair", "count")
    reduce_hashtag_df
  }


  def popularSet(spark: SparkSession): Set[String] = {
    val popular_hashtag_file = "wasb://yingliucontainer@yingliuzhizhu.blob.core.windows.net/popular_hashtags.txt"
    //    val popular_hashtag_file = "file:///Users/qiuchenzhang/Code/CMU/15619/Ying_Liu_Zhi_Zhu-S20/phase1/twitter/ETL/cleanHashtag/popular_hashtags.txt"
    val popular_hashtag_df = spark.read.csv(popular_hashtag_file).withColumnRenamed("_c0", "hashtag")
    val popular_set = popular_hashtag_df.select("hashtag").collect().map(r => r.getString(0).toLowerCase(Locale.ENGLISH)).toSet
    popular_set
  }

  def saveCSV(retDf: DataFrame): Unit = {
    retDf.write.mode("overwrite"). // overwrite old result
      option("sep", "⊢").
      option("header", "false").
      csv("wasb:///allHashtags")
  }

  def readCSV(hashtagUrl: String, spark: SparkSession): DataFrame = {
    val hashtagSchema = StructType(
      Array(
        StructField("sender_uid", LongType, false),
        StructField("hashtags", StringType, false)
      )
    )
    val hashtagDf = spark.read.schema(hashtagSchema).
      option("sep", "⊢").
      option("multiLine", "true").
      option("header", "false").
      csv(hashtagUrl)
    hashtagDf
  }


  /**
   * @param args it should be called with two arguments, the input path, and the output path.
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.sparkSession()
    val popSet = popularSet(spark)
    //    val input_url = "file:///Users/qiuchenzhang/Code/CMU/15619/Ying_Liu_Zhi_Zhu-S20/phase1/twitter/ETL/output/testOutput/tweetDf"
    //    val input_url = "wasb://yingliucontainer@yingliuzhizhu.blob.core.windows.net/tweetDf/"
    //    val hashtagRdd : RDD[(Long, String)] = cleanHashtag(input_url, spark)

    //    val retDf: DataFrame = concat(hashtagRdd, spark, popSet)
    //    val retDf: DataFrame = count(hashtagRdd, spark, popSet)
    val hashtagUrl = "wasb://yingliucontainer@yingliuzhizhu.blob.core.windows.net/hashtagDf/"
    val hashtagDf = readCSV(hashtagUrl, spark)
    loadToMySQL(hashtagDf)
    //    saveCSV(retDf)


  }
}
