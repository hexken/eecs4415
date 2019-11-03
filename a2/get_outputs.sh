rm -rf ureducer.txt breducer.txt treducer.txt

hdfs dfs -get /ureducer/part* ureducer.txt
hdfs dfs -get /breducer/part* breducer.txt
hdfs dfs -get /treducer/part* treducer.txt
