for f in allHashtags/*.csv
do
    mysql -e "LOAD DATA INFILE '"$f"' INTO TABLE all_clean_hashtag
      FIELDS TERMINATED BY '⊢' LINES TERMINATED BY '\n' " 
      -u root --password= password all_db
echo "Done: '"$f"' at $(date)"
done