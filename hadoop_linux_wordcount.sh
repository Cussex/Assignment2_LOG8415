#!/bin/bash -i

sudo -i

if [[ ! -d "input" ]]; then 
    mkdir input
else 
    rm -f input/*
fi

if [[ -d "output" ]]; then 
    rm -rf output
fi

if [[ ! -d "results" ]]; then 
    mkdir results
else 
    rm -f results/*
fi

if [[ ! -d "/home/ubuntu/results" ]]; then 
    mkdir /home/ubuntu/results
else 
    rm -f /home/ubuntu/results/*
fi

curl -O -s https://www.gutenberg.org/cache/epub/4300/pg4300.txt
mv pg4300.txt input/

source ~/.profile
echo pg4300.txt >> results/hadoop_wordcount_time.txt
echo pg4300.txt >> results/linux_wordcount_time.txt
{ time -p hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar wordcount input output 2> hadoop_log.txt; } 2>> results/hadoop_wordcount_time.txt
{ time -p cat input/pg4300.txt | tr ' ' '\n'  | sort | uniq -c 1> linux_log.txt; } 2>> results/linux_wordcount_time.txt

cp -r results/* /home/ubuntu/results