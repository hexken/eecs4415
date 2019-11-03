# clear working directory and hdfs output dirs
hdfs dfs -rm -r -f /ureducer
hdfs dfs -rm -r -f /breducer
hdfs dfs -rm -r -f /treducer
rm -rf ureducer.txt breducer.txt treducer.txt

hadoop jar /usr/hadoop-3.0.0/share/hadoop/tools/lib/hadoop-streaming-3.0.0.jar \
-file ./umapper.py \
-mapper ./umapper.py \
-file ./ureducer.py \
-reducer ./ureducer.py \
-input /small_tips.csv \
-output /ureducer

hadoop jar /usr/hadoop-3.0.0/share/hadoop/tools/lib/hadoop-streaming-3.0.0.jar \
-file ./bmapper.py \
-mapper ./bmapper.py \
-file ./breducer.py \
-reducer ./breducer.py \
-input /small_tips.csv \
-output /breducer

hadoop jar /usr/hadoop-3.0.0/share/hadoop/tools/lib/hadoop-streaming-3.0.0.jar \
-file ./tmapper.py \
-mapper ./tmapper.py \
-file ./treducer.py \
-reducer ./treducer.py \
-input /small_tips.csv \
-output /treducer

# get results from hdfs
hdfs dfs -get /ureducer/part* ureducer.txt
hdfs dfs -get /breducer/part* breducer.txt
hdfs dfs -get /treducer/part* treducer.txt
