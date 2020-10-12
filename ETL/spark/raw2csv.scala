import java.util.Properties
import java.sql.DriverManager
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


/* Choice */
// val choice = 1 // single file
// val choice = 0 // all files
val choice = -1 // test file

/* Extract */
val test_file = "hdfs:///input/query2_test.txt"
val one_file = "gs://cmuccpublicdatasets/twitter/s20/part-r-00000.gz" 
val all_files = "gs://cmuccpublicdatasets/twitter/s20/*.gz" 

val filename = if (choice == 1) one_file else if (choice == 0) all_files else test_file

val df = spark.read.json(filename)


/* Transform */
// 1. convert time to timetsamp
// 2. concatenate hashtags with ,
val selectDf = df.select($"id" as "tid",
                         $"id_str" as "tid_str",
                         unix_timestamp(col("created_at"),
                         "EEE MMM dd HH:mm:ss ZZZZ yyyy") as "timestamp", 
                         $"text" as "content", 
                         $"in_reply_to_user_id" as "reply_to_uid",
                         $"in_reply_to_user_id_str" as "reply_to_uid_str", 
                         $"user.id" as "sender_uid", 
                         $"user.id_str" as "sender_uid_str", 
                         $"user.screen_name" as "sender_screen_name",
                         $"user.description" as "sender_description",
                         $"retweeted_status.user.id" as "retweet_to_uid", 
                         $"retweeted_status.user.id_str" as "retweet_to_uid_str",
                         $"retweeted_status.user.screen_name" as "retweet_to_uid_screen_name",
                         $"retweeted_status.user.description" as "retweet_to_uid_description",
                         concat_ws(",", $"entities.hashtags.text") as "hashtags", 
                         $"lang")

// filter out malformed tweets and tweets not using specific languages
// 1. Cannot be parsed as a JSON object
// 2. Both id and id_str of the tweet object are missing or null
// 3. Both id and id_str of the user object are missing or null
// 4. created_at is missing or null
// 5. text is missing or null or empty_string
// 6. hashtag array missing or null or of length zero/empty
val filterDf = selectDf.filter(($"lang" isin ("ar", "en", "fr", "in", "pt", "es", "tr", "ja")) &&
                                  ($"tid".isNotNull || $"tid_str".isNotNull) &&
                                  ($"sender_uid".isNotNull ||  $"sender_uid_str".isNotNull) &&
                                  $"timestamp".isNotNull &&
                                  $"content".isNotNull &&
                                  $"hashtags".isNotNull && (length($"hashtags") > 0))
                      

// combine user_id and user_str
val cleanDf = filterDf.withColumn("tid", when($"tid_str".isNotNull, $"tid_str").otherwise($"tid")).
                       withColumn("sender_uid", when($"sender_uid_str".isNotNull, $"sender_uid_str").otherwise($"sender_uid")).
                       withColumn("reply_to_uid", when($"reply_to_uid_str".isNotNull, $"reply_to_uid_str").otherwise($"reply_to_uid")).
                       withColumn("retweet_to_uid", when($"retweet_to_uid_str".isNotNull, $"retweet_to_uid_str").otherwise($"retweet_to_uid")).
                       drop("tid_str", "sender_uid_str", "reply_to_uid_str", "retweet_to_uid_str")


// tweetDf is for tweet_table
val tweetDf = cleanDf.drop("sender_screen_name", "sender_description", "retweet_to_uid_screen_name", "retweet_to_uid_description")
// userDf is for user_table
val senderDf = cleanDf.select("sender_uid", "sender_screen_name", "sender_description", "timestamp")

val userMidDf = cleanDf.select($"retweet_to_uid" as "uid", 
                            $"retweet_to_uid_screen_name" as "screen_name", 
                            $"retweet_to_uid_description" as "description", 
                            $"timestamp").
                     filter($"uid".isNotNull).
                     union(senderDf)

// find the latest information of each users
// may cost a lot of time
// only execute when running one file
var userDf = sc.parallelize(Array("uid", "screen_name", "description", "lastestTime")).toDF()
if (choice != 0) {
    val w =Window.partitionBy($"uid")
    userDf = userMidDf.withColumn("lastestTime", max("timestamp").over(w)).
                              filter($"lastestTime" === $"timestamp").
                              drop("timestamp")
} 


/* Load */
// Load to intermediate file
// save to intermediate file, which dilimiter should use ???

val testOutput = "/raw2csv/testoutput/"
val oneOutput = "/raw2csv/oneOutput/"
val allOutput = "/raw2csv/allOutput/"

val output = if (choice == 1) oneOutput else if (choice == 0) allOutput else testOutput

tweetDf.write.mode("overwrite").  // overwrite old result
              option("sep","⊢").
              option("header","true").
              csv(output + "tweetOutput")
selectDf.write.mode("overwrite").
             option("decoding", "unicode").
             option("sep","⊢").
             option("header","true").
             csv(output + "selectDf")
filterDf.write.mode("overwrite").
             option("sep","⊢").
             option("header","true").
             csv(output + "filterDf")
userMidDf.write.mode("overwrite").
             option("sep","⊢").
             option("header","true").
             csv(output + "userMidDf")
userDf.write.mode("overwrite").
             option("encoding", "UTF-8").
             option("sep","⊢").
             option("header","true").
             csv(output + "userOutput")


// read intermediate file
// val readTweetDf = spark.read.option("sep","⊢").           // strange delimiter
//                              option("header", "true").    // save header
//                              option("multiLine", true).   // deal with /n in fields
//                              csv(output + "tweetOutput/*.csv")
// val readUserDf = spark.read.option("sep","⊢").
//                             option("multiLine", true).
//                             option("header", "true").
//                             csv(output + "userOutput/*.csv")
// readTweetDf.show()
// readUserDf.show()


// Load to MySQL
// set credentials
val jdbcPort = "3306"
val jdbcDatabase = ""
val jdbcUsername = ""
val jdbcPassword = ""   // the password you set
val jdbcHostname = "35.237.76.83"   // the external IP address of you MySQL DB

val jdbcUrl =s"jdbc:mysql://$jdbcHostname:$jdbcPort/$jdbcDatabase" + "?serverTimezone=UTC"

val driverClass = "com.mysql.jdbc.Driver"
Class.forName(driverClass)  // check jdbc driver

// set connection properties
val connectionProperties = new Properties()
connectionProperties.put("user", s"$jdbcUsername")
connectionProperties.put("password", s"$jdbcPassword")
connectionProperties.setProperty("Driver", driverClass)
connectionProperties.setProperty("useServerPrepStmts", "false")    // take note of this configuration, and understand what it does
connectionProperties.setProperty("rewriteBatchedStatements", "true")  // take note of this configuration, and understand what it does

// first drop the table if it already exists
val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
assert(!connection.isClosed)
val stmt = connection.createStatement()
stmt.executeUpdate("drop table if exists tweet_table")
stmt.executeUpdate("drop table if exists user_table")

// write the dataframe to a table called "test_twitter"
tweetDf.write.option("encoding", "UTF-8").jdbc(jdbcUrl, "tweet_table", connectionProperties)
userDf.write.option("encoding", "UTF-8").jdbc(jdbcUrl, "user_table", connectionProperties)

