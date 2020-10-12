for f in /var/lib/mysql-files/query3_output/*.csv
do
    mysql -e "LOAD DATA INFILE '"$f"' INTO TABLE query3_table FIELDS TERMINATED BY '⊢' LINES TERMINATED BY '\n' " -u root -ppassword all_db
echo "Done: '"$f"' at $(date)"
done