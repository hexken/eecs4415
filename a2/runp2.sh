#!/bin/bash
# clear working directory and hdfs output dirs

if [ $# -eq 0 ]
then
    echo 'No input file provided'
    exit
fi

input_file=$1

hdfs dfs -rm -r -f /checkins
rm -rf checkinsbyday.txt

hadoop jar /usr/hadoop-3.0.0/share/hadoop/tools/lib/hadoop-streaming-3.0.0.jar \
-file ./checkinsmapper.py \
-mapper ./checkinsmapper.py \
-file ./checkinsreducer.py \
-reducer ./checkinsreducer.py \
-input /$input_file \
-output /checkins


# get results from hdfs
hdfs dfs -get /checkins/part* checkinsbyday.txt
