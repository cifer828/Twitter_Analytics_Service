-- combine all_latest and all_interaction
DROP TABLE IF EXISTS combine_latest_interaction_tmp;
CREATE TABLE combine_latest_interaction_tmp (
	first_uid BIGINT NOT NULL,
	second_uid BIGINT NOT NULL,
	latest_tweet VARCHAR(1023) NOT NULL,
	interaction INT,
	INDEX (first_uid)
)
SELECT CAST(SUBSTRING_INDEX(all_latest.pair, '_', 1) AS UNSIGNED) AS first_uid, 
CAST(SUBSTRING_INDEX(all_latest.pair, '_', -1) AS UNSIGNED) AS second_uid, 
all_latest.content as latest_tweet, 
all_interaction.count as interaction from all_latest
JOIN all_interaction
ON all_latest.pair = all_interaction.pair;

DROP TABLE IF EXISTS combine_latest_interaction;
CREATE TABLE combine_latest_interaction (
	uid BIGINT NOT NULL,
	contact_uid BIGINT NOT NULL,
	latest_tweet VARCHAR(1023) NOT NULL,
	interaction INT,
	INDEX (uid)
)
SELECT first_uid as uid, second_uid as contact_uid, latest_tweet, interaction
FROM combine_latest_interaction_tmp
UNION ALL
SELECT second_uid as uid, first_uid as contact_uid, latest_tweet, interaction
FROM combine_latest_interaction_tmp;

-- combine all_user_table 
DROP TABLE IF EXISTS combine_user;
CREATE TABLE combine_user (
	uid BIGINT NOT NULL,
	contact_uid BIGINT NOT NULL,
	latest_tweet VARCHAR(1023) NOT NULL,
	interaction INT,
	screen_name TEXT,
	description TEXT,
	INDEX (uid)
)
SELECT combine.*, user.screen_name, user.description FROM combine_latest_interaction combine
LEFT JOIN all_user_table user
ON combine.contact_uid = user.uid;

-- combine all_contact_reply
DROP TABLE IF EXISTS combine_reply;
CREATE TABLE combine_reply (
	uid BIGINT NOT NULL,
	contact_uid BIGINT NOT NULL,
	content MEDIUMTEXT NOT NULL,
	hashtags MEDIUMTEXT NOT NULL,
	latest_tweet VARCHAR(1023) NOT NULL,
	interaction INT NOT NULL,
	screen_name TEXT,
	description TEXT,
	INDEX (uid)
)
SELECT 
combine.*,
reply.hashtags, 
reply.content
from all_contact_reply reply
LEFT JOIN combine_user combine
ON reply.uid = combine.uid AND reply.contact_uid = combine.contact_uid;

DROP TABLE IF EXISTS combine_retweet;
CREATE TABLE combine_retweet (
	uid BIGINT NOT NULL,
	contact_uid BIGINT NOT NULL,
	content MEDIUMTEXT NOT NULL,
	hashtags MEDIUMTEXT NOT NULL,
	latest_tweet VARCHAR(1023) NOT NULL,
	interaction INT NOT NULL,
	screen_name TEXT,
	description TEXT,
	INDEX (uid)
)
SELECT 
combine.*,
retweet.hashtags, 
retweet.content
from all_contact_retweet retweet
LEFT JOIN combine_user combine
ON retweet.uid = combine.uid AND retweet.contact_uid = combine.contact_uid;

show global variables like 'innodb_buffer%';
