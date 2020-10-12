#!/usr/bin/env python
# -*- coding: utf-8 -*-
import locale
import mysql.connector
from mysql.connector import errorcode
from datetime import datetime

locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

"""
author: Cifer Z.
date: 4/22/20
"""


def compare_hahtags(uid1, uid2, ht_dict):
    count = 0
    if uid1 not in ht_dict or uid2 not in ht_dict:
        return count
    dict1 = ht_dict[uid1]
    dict2 = ht_dict[uid2]
    for (key, value) in dict1.items():
        if key in dict2:
            count += dict1[key] + dict2[key]
    return count


def time_str():
    return datetime.now().strftime("%m/%d/%Y, %H:%M:%S")

cnx = mysql.connector.connect(
    host="localhost",
    user="",
    passwd="",
    database=''
)

cursor = cnx.cursor()


inter_dict = {}
hashtag_dict = {}
user_dict = {}

print("{}: loading fifty_clean_hashtag ...".format(time_str()))
hashtag_query = "SELECT * FROM fifty_clean_hashtag"
cursor.execute(hashtag_query)
i = 0
for (sender_uid, hashtags) in cursor:
    if i % 1000000 == 0:
        print("read {} rows".format(i))
    hash_dict = {}
    for ht in hashtags.split(","):
        hash_dict[ht.lower()] = hash_dict.get(ht.lower(), 0) + 1
    hashtag_dict[int(sender_uid)] = hash_dict
    i += 1
print("{}: loaded {} rows from fifty_clean_hashtag\n".format(time_str(), i))

print("{}: loading fifty_user_table ...".format(time_str()))
hashtag_query = "SELECT uid, screen_name, description FROM fifty_user_table"
cursor.execute(hashtag_query)
i = 0
for (uid, screen_name, description) in cursor:
    if i % 1000000 == 0:
        print("read {} rows".format(i))
    if screen_name is None:
        screen_name = ""
    if description is None:
        description = ""
    user_dict[int(uid)] = (screen_name, description)
    i += 1
print("{}: loaded {} rows from fifty_user_table\n".format(time_str(), i))

# 0 for reply
print("{}: loading fifty_contact_reply ...".format(time_str()))
reply_query = "SELECT * FROM fifty_contact_reply"
cursor.execute(reply_query)
i = 0
for (uid, contact_uid, hashtags, content) in cursor:
    if i % 1000000 == 0:
        print("read {} rows".format(i))
    pair = "{}_{}_0".format(min([uid, contact_uid]), max([uid, contact_uid]))
    inter_dict[pair] = {"ht": hashtags,
                        "con": content,
                        "ht_cnt": compare_hahtags(uid, contact_uid, hashtag_dict)}
    i += 1
print("{}: loaded {} rows from fifty_contact_reply\n".format(time_str(), i))

# 1 for retweet
print("{}: loading fifty_contact_retweet ...".format(time_str()))
retweet_query = "SELECT * FROM fifty_contact_retweet"
cursor.execute(retweet_query)
i = 0
for (uid, contact_uid, hashtags, content) in cursor:
    if i % 1000000 == 0:
        print("read {} rows".format(i))
    pair = "{}_{}_1".format(min([uid, contact_uid]), max([uid, contact_uid]))
    inter_dict[pair] = {"ht": hashtags,
                        "con": content,
                        "ht_cnt": compare_hahtags(uid, contact_uid, hashtag_dict)}
    i += 1
print("{}: loaded {} rows from fifty_contact_retweet\n".format(time_str(), i))

print("{}: loading fifty_interaction ...".format(time_str()))
inter_query = "SELECT * FROM fifty_interaction"
cursor.execute(inter_query)
i = 0
for (pair, count) in cursor:
    if i % 1000000 == 0:
        print("read {} rows".format(i))
    # reply
    if pair + "_0" in inter_dict:
        inter_dict[pair + "_0"]["n"] = count
    # reweet
    if pair + "_1" in inter_dict:
        inter_dict[pair + "_1"]["n"] = count
    i += 1
print("{}: loaded {} rows from fifty_interaction\n".format(time_str(), i))

print("{}: loading fifty_latest ...".format(time_str()))
latest_query = "SELECT pair, content FROM fifty_latest"
cursor.execute(latest_query)
i = 0
for (pair, content) in cursor:
    if i % 1000000 == 0:
        print("read {} rows".format(i))
    # reply
    if pair + "_0" in inter_dict:
        inter_dict[pair + "_0"]["l"] = content
    # reweet
    if pair + "_1" in inter_dict:
        inter_dict[pair + "_1"]["l"] = content
    i += 1
print("{}: loaded {} rows from fifty_latest\n".format(time_str(), i))

DB_NAME = "fifty_db"
def create_database(cursor):
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)
try:
    cursor.execute("USE {}".format(DB_NAME))
except mysql.connector.Error as err:
    print("Database {} does not exists.".format(DB_NAME))
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        create_database(cursor)
        print("Database {} created successfully.".format(DB_NAME))
        cnx.database = DB_NAME
    else:
        print(err)
        exit(1)

TABLES = {}
TABlE_NAME = "python_combine_all"
TABLES[TABlE_NAME] = (
    "CREATE TABLE `{}` ("
    "  `uid` BIGINT NOT NULL,"
    "  `contact_uid` BIGINT NOT NULL,"
    "  `type` INT NOT NULL,"
    "  `interaction` INT NOT NULL,"
    "  `latest_tweet` varchar(1023) NOT NULL,"
    "  `hashtag_cnt` INT NOT NULL,"
    "  `hashtags` MEDIUMTEXT NOT NULL,"
    "  `content` MEDIUMTEXT NOT NULL,"
    "  `screen_name` TEXT,"
    "  `description` TEXT,"
    "  INDEX(uid)"
    ") ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci".format(TABlE_NAME))


def create_table(table_name):
    table_description = TABLES[table_name]
    try:
        print("Creating table {}: ".format(table_name), end='')
        cursor.execute("DROP TABLE IF EXISTS {}".format(table_name))
        cursor.execute(table_description)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")


print("{}: combine all data".format(time_str()))
combine_list = []
for key, value in inter_dict.items():
    uid1 = int(key.split("_")[0])
    uid2 = int(key.split("_")[1])
    ty = int(key.split("_")[2])
    combine_list.append((uid1, uid2, ty, value["n"], value["l"], value["ht_cnt"], value["ht"], value["con"],
                         *user_dict.get(uid2, ("", ""))))
    if uid1 != uid2:
        combine_list.append((uid2, uid1, ty, value["n"], value["l"], value["ht_cnt"], value["ht"], value["con"],
                             *user_dict.get(uid1, ("", ""))))
print("{}: finish combining all data".format(time_str()))

print("{}: start loading {}".format(time_str(), TABlE_NAME))
create_table(TABlE_NAME)
sql = "INSERT INTO {} (uid, contact_uid, type, interaction, latest_tweet, hashtag_cnt, hashtags, content, screen_name, description) " \
      "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".format(TABlE_NAME)
for i in range(0, len(combine_list), 50000):
    print("loaded {} rows".format(i))
    cursor.executemany(sql, combine_list[i: i + 50000])
print("{}: finish loading {} rows".format(time_str(), len(combine_list)))

cnx.commit()
cursor.close()
cnx.close()
