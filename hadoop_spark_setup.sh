#!/bin/bash -i

apt-get update
apt-get -y upgrade
apt install openjdk-8-jdk -y scala python3-pip -y
pip3 install --no-cache-dir pyspark

ping -c 5 dlcdn.apache.org 2>> log.txt
nslookup dlcdn.apache.org 2>> log.txt

#Install hadoop 
curl -O -L --retry 10 --retry-delay 5 https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz 2>> log.txt
tar -xzvf hadoop-3.3.6.tar.gz
mkdir /usr/local/hadoop
mv hadoop-3.3.6/* /usr/local/hadoop
rm -rf hadoop-3.3.6 hadoop-3.3.6.tar.gz

ping -c 5 archive.apache.org 2>> log.txt
nslookup archive.apache.org 2>> log.txt

#Install spark 
curl -O -L --retry 10 --retry-delay 5 https://archive.apache.org/dist/spark/spark-2.0.0/spark-2.0.0-bin-without-hadoop.tgz 2>> log.txt
tar -xzvf spark-2.0.0-bin-without-hadoop.tgz 
mkdir /opt/spark 
mv spark-2.0.0-bin-without-hadoop/* /opt/spark 
rm -rf spark-2.0.0-bin-without-hadoop spark-2.0.0-bin-without-hadoop.tgz

# Setup hadoop config
cat <<EOF > /usr/local/hadoop/etc/hadoop/hadoop-env.sh
export JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:bin/java::")
export HADOOP_HOME=/usr/local/hadoop
EOF

# Set env variables  
echo export JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:bin/java::") >> ~/.profile
echo export HADOOP_HOME=/usr/local/hadoop >> ~/.profile
echo export SPARK_HOME=/opt/spark >> ~/.profile
echo export PYSPARK_PYTHON=/usr/bin/python3 >> ~/.profile
echo export PATH=$PATH:/opt/spark/bin:/opt/spark/sbin:/usr/local/hadoop/bin >> ~/.profile
source ~/.profile
echo export SPARK_DIST_CLASSPATH=$(hadoop classpath) >> ~/.profile
source ~/.profile

# Start spark
start-master.sh
