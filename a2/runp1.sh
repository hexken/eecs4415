#!/bin/bash

if [ $# -eq 0 ]
then
    echo 'No input supplied'
    exit
fi

input_file=$1

# clear working directory and hdfs output dirs
hdfs dfs -rm -r -f /unigrams
hdfs dfs -rm -r -f /bigrams
hdfs dfs -rm -r -f /trigrams
rm -rf ureducer.txt breducer.txt treducer.txt

hadoop jar /usr/hadoop-3.0.0/share/hadoop/tools/lib/hadoop-streaming-3.0.0.jar \
-file ./umapper.py \
-mapper ./umapper.py \
-file ./ureducer.py \
-reducer ./ureducer.py \
-input /$input_file \
-output /unigrams

hadoop jar /usr/hadoop-3.0.0/share/hadoop/tools/lib/hadoop-streaming-3.0.0.jar \
-file ./bmapper.py \
-mapper ./bmapper.py \
-file ./breducer.py \
-reducer ./breducer.py \
-input /$input_file \
-output /bigrams

hadoop jar /usr/hadoop-3.0.0/share/hadoop/tools/lib/hadoop-streaming-3.0.0.jar \
-file ./tmapper.py \
-mapper ./tmapper.py \
-file ./treducer.py \
-reducer ./treducer.py \
-input /$input_file \
-output /trigrams

# get results from hdfs
hdfs dfs -get /unigrams/part* unigrams.txt
hdfs dfs -get /bigrams/part* bigrams.txt
hdfs dfs -get /trigrams/part* trigrams.txt
