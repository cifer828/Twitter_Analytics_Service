CREATE INDEX sender_uid ON all_clean_hashtag_3 (sender_uid);
CREATE INDEX uid ON all_contact_reply_3 (uid);
CREATE INDEX uid ON all_contact_retweet_3 (uid);
CREATE INDEX pair ON all_interaction_3 (pair);
CREATE INDEX pair ON all_latest_3 (pair);
CREATE INDEX uid ON all_user_table_3 (uid);
CREATE INDEX uid_timestamp ON query3_3 (uid, timestamp);

SHOW INDEX FROM all_clean_hashtag_3;
SHOW INDEX FROM all_contact_reply_3;
SHOW INDEX FROM all_contact_retweet_3;
SHOW INDEX FROM all_interaction_3;
SHOW INDEX FROM all_latest_3;
SHOW INDEX FROM all_user_table_3;

SELECT COUNT(*) FROM all_clean_hashtag_3;
SELECT COUNT(*) FROM all_contact_reply_3;
