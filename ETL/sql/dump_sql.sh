echo "Start dumping at $(date)"

mysqldump -uroot -ppassword all_db all_clean_hashtag_3> all_clean_hashtag_3.sql
echo "Done: all_clean_hashtag_3 at $(date)"

mysqldump -uroot -ppassword all_db all_contact_reply_3> all_contact_reply_3.sql
echo "Done: all_contact_reply_3 at $(date)"

mysqldump -uroot -ppassword all_db all_contact_retweet_3> all_contact_retweet_3.sql
echo "Done: all_contact_retweet_3 at $(date)"

mysqldump -uroot -ppassword all_db all_interaction_3> all_interaction_3.sql
echo "Done: all_interaction_3 at $(date)"

mysqldump -uroot -ppassword all_db all_latest_3> all_latest_3.sql
echo "Done: all_latest_3 at $(date)"

mysqldump -uroot -ppassword all_db all_user_table_3> all_user_table_3.sql
echo "Done: all_user_table_3 at $(date)"

mysqldump -uroot -ppassword all_db query3_3> query3_3.sql
echo "Done: query3_3 at $(date)"
