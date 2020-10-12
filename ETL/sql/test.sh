echo "Start loading at $(date)"

mysql -uroot -ppassword all_db < all_contact_retweet.sql
echo "Done: all_contact_retweet at $(date)"

mysql -uroot -ppassword all_db < all_clean_hashtag.sql
echo "Done: all_clean_hashtag at $(date)"

echo "Done: all_clean_hashtag at $(date)"
mysql -uroot -ppassword all_db < all_clean_hashtag_3.sql
echo "Done: all_clean_hashtag_3 at $(date)"


