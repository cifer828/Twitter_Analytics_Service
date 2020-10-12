# dump fifty dataset
mysqldump -uroot -ppassword fifty_db fifty_tweet_table > sql/fifty_tweet_table.sql 
mysqldump -uroot -ppassword fifty_db fifty_user_table > sql/fifty_user_table.sql 
# dump all dataset
mysqldump -uroot -ppassword all_db all_contact_reply > sql/all_contact_reply.sql 
mysqldump -uroot -ppassword all_db all_contact_retweet > sql/all_contact_retweet.sql 
mysqldump -uroot -ppassword all_db all_interaction > sql/all_interaction.sql 
mysqldump -uroot -ppassword all_db all_latest > sql/all_latest.sql 
mysqldump -uroot -ppassword all_db all_user_table > sql/all_user_table.sql 
mysqldump -uroot -ppassword all_db all_tweet_table > sql/all_tweet_table.sql 
# upload to s3
aws s3 sync sql s3://twitter-yingliuzhizhu/new-sql