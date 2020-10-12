-- 0. Create INDEX
CREATE INDEX reply_to_uid ON all_tweet_table (reply_to_uid);
CREATE INDEX retweet_to_uid ON all_tweet_table (retweet_to_uid);
CREATE INDEX sender_uid ON all_tweet_table (sender_uid);
-- primary key!!!
ALTER TABLE all_tweet_table ADD PRIMARY KEY(tid);

-- 1. remove duplicates
-- 1.1 create a table to save duplicate values
DROP TABLE IF EXISTS all_duplicate;
SET @row := 0;
CREATE TABLE all_duplicate (
	`row_id` INT,
  `tid` BIGINT,
  `timestamp` BIGINT,
  `content` VARCHAR(1023),
  `reply_to_uid` BIGINT,
  `sender_uid` BIGINT,
	`retweet_to_uid` BIGINT,
  `hashtags` VARCHAR(255),
  `lang` VARCHAR(255), 
	INDEX (tid)
) ENGINE=INNODB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
SELECT @row:=@row+1 AS row_id, tid, timestamp, content, reply_to_uid, sender_uid, retweet_to_uid, hashtags, lang 
FROM all_tweet_table
WHERE tid in 
(SELECT tid FROM all_tweet_table GROUP BY tid HAVING count(*) > 1);
-- 1.2 remove duplicates using duplicate table
DELETE t1 FROM all_duplicate t1, all_duplicate t2 WHERE t1.tid = t2.tid AND t1.row_id < t2.row_id;
DELETE FROM all_tweet_table t WHERE t.tid IN (SELECT tid FROM all_duplicate);
INSERT INTO all_tweet_table SELECT tid, timestamp,  content, reply_to_uid, sender_uid, retweet_to_uid, hashtags, lang FROM all_duplicate;


-- 2. create reply TABLE
DROP TABLE IF EXISTS all_reply;
CREATE TABLE all_reply (
  `tid` BIGINT NOT NULL,
  `timestamp` BIGINT DEFAULT NULL,
  `content` VARCHAR(1024) NOT NULL,
  `reply_to_uid` BIGINT NOT NULL,
  `sender_uid` BIGINT NOT NULL,
  `hashtags` VARCHAR(255) NOT NULL,
  `lang` VARCHAR(255), 
-- 	INDEX (sender_uid),
-- 	INDEX (reply_to_uid),
-- 	INDEX (sender_uid, reply_to_uid),
	PRIMARY KEY (tid)
) ENGINE=INNODB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
SELECT tid, timestamp, content, reply_to_uid, sender_uid, hashtags, lang FROM all_tweet_table
WHERE reply_to_uid IS NOT NULL; 

-- 3. create retweet TABLE
DROP TABLE IF EXISTS all_retweet;
CREATE TABLE all_retweet (
  `tid` bigint NOT NULL,
  `timestamp` bigint DEFAULT NULL,
  `content` VARCHAR(1024) NOT NULL,
  `retweet_to_uid` bigint NOT NULL,
  `sender_uid` bigint NOT NULL,
  `hashtags` VARCHAR(255) NOT NULL,
  `lang` VARCHAR(255),
-- 	INDEX (sender_uid),
-- 	INDEX (retweet_to_uid),
-- 	INDEX (sender_uid, retweet_to_uid),
	PRIMARY KEY (tid)
) ENGINE=INNODB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
SELECT tid, timestamp, content, retweet_to_uid, sender_uid, hashtags, lang FROM all_tweet_table
WHERE retweet_to_uid IS NOT NULL;

-- 4. count check
select count(*) from all_reply;
select count(*) from all_retweet;
select count(*) from all_tweet_table;
SHOW INDEX FROM all_reply;
SHOW INDEX FROM all_retweet;
SHOW INDEX FROM all_tweet_table;

-- 5.build interaction table
-- 5.1 Select reply pair and count
DROP TABLE IF EXISTS reply_tmp;
CREATE TABLE reply_tmp(
	`reply_pair` VARCHAR(255),
	`reply_count` INT,
	PRIMARY KEY (reply_pair)
)
SELECT reply_pair, COUNT(*) AS reply_count
FROM(
SELECT CONCAT(LEAST(sender_uid, reply_to_uid), '_', GREATEST(sender_uid, reply_to_uid)) AS reply_pair
FROM all_reply 
) t
GROUP BY reply_pair ORDER BY NULL; -- use ORDER BY NULL to prevent GROUP BY sorting the result

-- 5.2 Select retweet pair and count
DROP TABLE IF EXISTS retweet_tmp;
CREATE TABLE retweet_tmp(
	`retweet_pair` VARCHAR(255),
	`retweet_count` INT,
	PRIMARY KEY (retweet_pair)
)
SELECT retweet_pair, COUNT(*) AS retweet_count
FROM(
SELECT CONCAT(LEAST(sender_uid, retweet_to_uid), '_', GREATEST(sender_uid, retweet_to_uid)) AS retweet_pair
FROM all_retweet
) t
GROUP BY retweet_pair ORDER BY NULL;

-- 5.3 full outer join the reply and retweet count
DROP TABLE IF EXISTS all_interaction;
CREATE TABLE all_interaction(
	`pair` VARCHAR(255),
	`count` INT,
	PRIMARY KEY (pair)
) 
SELECT IFNULL(reply_pair, retweet_pair) as pair, IFNULL(reply_count,0) * 2 + IFNULL(retweet_count, 0) as count
FROM (
SELECT *
FROM reply_tmp t1 
LEFT JOIN retweet_tmp t2
ON t1.reply_pair = t2.retweet_pair
UNION
SELECT *
FROM reply_tmp t1 
RIGHT JOIN retweet_tmp t2
ON t1.reply_pair = t2.retweet_pair 
) t;

-- 6. hashtag table
-- default size of GROUP_CONCAT is 1024
SET SESSION group_concat_max_len = 600000000;
-- DROP TABLE IF EXISTS all_hashtags;
-- CREATE TABLE all_hashtags(
-- 	sender_uid BIGINT,
-- 	hashtags text,
-- 	PRIMARY KEY (sender_uid)
-- ) 
-- SELECT sender_uid, GROUP_CONCAT(hashtags SEPARATOR ',') hashtags
-- FROM all_tweet_table
-- GROUP BY sender_uid ORDER BY NULL;

-- 7. keyword table
-- 7.1 reply keywords
DROP TABLE IF EXISTS all_keyword_reply;
CREATE TABLE all_keyword_reply(
	pair VARCHAR(255),
	content MEDIUMTEXT,
	hashtags MEDIUMTEXT,
	PRIMARY KEY (pair)
) 
SELECT pair, GROUP_CONCAT(hashtags SEPARATOR ',') hashtags, GROUP_CONCAT(content SEPARATOR ',') content
FROM (
SELECT CONCAT(LEAST(sender_uid, reply_to_uid), '_', GREATEST(sender_uid, reply_to_uid)) as pair, hashtags, content
FROM all_reply
) t
GROUP BY pair ORDER BY NULL;
-- 7.2 retweet keywords
DROP TABLE IF EXISTS all_keyword_retweet;
CREATE TABLE all_keyword_retweet(
	pair VARCHAR(255),
	content MEDIUMTEXT,
	hashtags MEDIUMTEXT,
	PRIMARY KEY (pair)
) 
SELECT pair, GROUP_CONCAT(hashtags SEPARATOR ',') hashtags, GROUP_CONCAT(content SEPARATOR ',') content
FROM (
SELECT CONCAT(LEAST(sender_uid, retweet_to_uid), '_', GREATEST(sender_uid, retweet_to_uid)) as pair, hashtags, content
FROM all_retweet
) t
-- 7.3 reply contact
DROP TABLE IF EXISTS all_contact_reply;
CREATE TABLE all_contact_reply(
	uid BIGINT,
	contact_uid BIGINT,
	content MEDIUMTEXT,
	hashtags MEDIUMTEXT,
	PRIMARY KEY (uid, contact_uid)
) 
SELECT uid, contact_uid, GROUP_CONCAT(hashtags SEPARATOR ',') hashtags, GROUP_CONCAT(content SEPARATOR ',') content
FROM (
SELECT sender_uid as uid, reply_to_uid as contact_uid, hashtags, content
FROM all_reply
UNION ALL
SELECT reply_to_uid as uid, sender_uid as contact_uid, hashtags, content
FROM all_reply
) t
GROUP BY uid, contact_uid ORDER BY NULL;
-- 7.4 retweet contact
DROP TABLE IF EXISTS all_contact_retweet;
CREATE TABLE all_contact_retweet(
	uid BIGINT,
	contact_uid BIGINT,
	content MEDIUMTEXT,
	hashtags MEDIUMTEXT,
	PRIMARY KEY (uid, contact_uid)
) 
SELECT uid, contact_uid, GROUP_CONCAT(hashtags SEPARATOR ',') hashtags, GROUP_CONCAT(content SEPARATOR ',') content
FROM (
SELECT sender_uid as uid, retweet_to_uid as contact_uid, hashtags, content
FROM all_retweet
UNION ALL
SELECT retweet_to_uid as uid, sender_uid as contact_uid, hashtags, content
FROM all_retweet
) t
GROUP BY uid, contact_uid ORDER BY NULL;

-- 8. lastest contact tweet table
-- 8.1 build TEMPORARY table of all pair tweets
DROP TEMPORARY TABLE IF EXISTS latest_tmp;
CREATE TABLE latest_tmp(
	tid BIGINT,
	pair VARCHAR(255),
	content VARCHAR(1023),
	timestamp BIGINT
) 
SELECT tid, CONCAT(LEAST(sender_uid, reply_to_uid), '_', GREATEST(sender_uid, reply_to_uid)) as pair, content, timestamp
FROM all_reply
UNION ALL
SELECT tid, CONCAT(LEAST(sender_uid, retweet_to_uid), '_', GREATEST(sender_uid, retweet_to_uid)) as pair, content, timestamp
FROM all_retweet;
-- 8.2 get the latest contact tweet of each pair
DROP TABLE IF EXISTS all_latest;
CREATE TABLE all_latest(
	tid BIGINT,
	pair VARCHAR(255),
	content VARCHAR(1023),
	timestamp BIGINT,
	PRIMARY KEY (pair)
) 
SELECT *
FROM (SELECT *,
             row_number() over (PARTITION BY pair ORDER BY timestamp DESC, tid DESC) AS seqnum
      FROM latest_tmp
     ) t
WHERE seqnum = 1
