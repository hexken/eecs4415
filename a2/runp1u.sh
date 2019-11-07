#!/bin/bash

if [ $# -eq 0 ]
then
    echo 'No input supplied'
    exit
fi

input_file=$1

# clear working directory and hdfs output dirs
rm unigrams.txt
hdfs dfs -rm -r -f /unigrams

hadoop jar /usr/hadoop-3.0.0/share/hadoop/tools/lib/hadoop-streaming-3.0.0.jar \
-file ./umapper.py \
-mapper ./umapper.py \
-file ./ureducer.py \
-reducer ./ureducer.py \
-input /$input_file \
-output /unigrams
# get results from hdfs
hdfs dfs -get /unigrams/part* unigrams.txt
