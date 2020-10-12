sudo iptables -t nat -A PREROUTING -p tcp --dport 80 -j REDIRECT --to-port 8080
sudo apt-get update
yes | sudo apt install openjdk-8-jdk
export MYSQL_DB=.
export MYSQL_USER=.
export MYSQL_PWD=.
wget https://twitter-yingliuzhizhu.s3.amazonaws.com/phase3/twitter-analytics-phase3.jar
wget https://twitter-yingliuzhizhu.s3.amazonaws.com/phase3/stopwords.txt
wget https://twitter-yingliuzhizhu.s3.amazonaws.com/phase3/decrypted_list.txt
java -jar twitter-analytics-phase3.jar &



 sudo lsof -i -P -n