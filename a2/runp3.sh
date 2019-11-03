#!/bin/bash
# clear working directory and hdfs output dirs

if [ $# -eq 0 ]
then
    echo 'No input file provided'
    exit
fi

input_file=$1

hdfs dfs -rm -r -f /inverted-index
rm -rf inverted-index.txt

hadoop jar /usr/hadoop-3.0.0/share/hadoop/tools/lib/hadoop-streaming-3.0.0.jar \
-file ./iimapper.py \
-mapper ./iimapper.py \
-file ./iireducer.py \
-reducer ./iireducer.py \
-input /$input_file \
-output /inverted-index


# get results from hdfs
hdfs dfs -get /inverted-index/part* inverted-index.txt
