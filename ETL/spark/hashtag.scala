import java.util.Properties
import java.sql.DriverManager
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import java.util.Locale
import org.apache.spark.sql.Row

val tweet_df_file = "file:///Users/qiuchenzhang/Code/CMU/15619/Ying_Liu_Zhi_Zhu-S20/phase1/twitter/ETL/output/testOutput/tweetDf"
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
                            csv(tweet_df_file)

val hashtag_df = tweet_df.select("sender_uid", "hashtags")

val hashtag_rdd = hashtag_df.map{
    case Row(sender_uid: Long, hashtags: String) => 
    (sender_uid, hashtags)
}

val popular_hashtag_file = "file:///Users/qiuchenzhang/Code/CMU/15619/Ying_Liu_Zhi_Zhu-S20/phase1/twitter/ETL/popular_hashtags.txt"
val popular_hashtag_df = spark.read.csv(popular_hashtag_file).withColumnRenamed("_c0", "hashtag")
val popular_set = popular_hashtag_df.select("hashtag").collect().map(r => r.getString(0).toLowerCase(Locale.ENGLISH)).toSet

hashtag_rdd.map{row => {
    val filterHt = Array()
    for (i <- 2 to row.size)
        if (!popular_set.contains(row(i)))
            filterHt +:= row(i).toString
    (row(1).toLong, filterHt)
}}
                        