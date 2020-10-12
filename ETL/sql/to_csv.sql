SELECT *
FROM fifty_hashtags_concat
INTO LOCAL OUTFILE '/Users/qiuchenzhang/Code/CMU/15619/Ying_Liu_Zhi_Zhu-S20/phase1/twitter/ETL/sql/fifty_hashtags_concat.csv'
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n';

show variables like 'secure%';

SELECT count(*) from combine_reply;
SELECT count(*) from combine_retweet;
SELECT count(*) from fifty_contact_reply;
SELECT count(*) from fifty_contact_retweet;

SELECT uid, contact_uid, count(*) from combine_user_latest_interaction
GROUP BY uid, contact_uid
HAVING count(*) > 1

SELECT SUBSTR(variable_value,1,
LOCATE(' ',variable_value) - 1) DBVersion
FROM information_schema.global_variables
WHERE variable_name='version_comment';