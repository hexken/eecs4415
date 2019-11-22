"""
    This Spark app connects to a script running on another (Docker) machine
    on port 9009 that provides a stream of raw tweets text. That stream is
    meant to be read and processed here, where top trending hashtags are
    identified. Both apps are designed to be run in Docker containers.

    To execute this in a Docker container, do:
    
        docker run -it -v $PWD:/app --link twitter:twitter eecsyorku/eecs4415

    and inside the docker:

        spark-submit spark_app.py

    For more instructions on how to run, refer to final tutorial 8 slides.

    Made for: EECS 4415 - Big Data Systems (York University EECS dept.)
    Modified by: Tilemachos Pechlivanoglou
    Based on: https://www.toptal.com/apache/apache-spark-streaming-twitter
    Original author: Hanee' Medhat

"""

from pyspark import SparkConf, SparkContext
import pandas as pd
from pyspark.streaming import StreamingContext
from pyspark import Row, SQLContext
import requests
import sys


# adding the count of each hashtag to its last count
def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

def update_df(x):
    print('{}    {}'.format(x[0], x[1]))
    df.loc[x[0]] = x[1]

def process_rdd(time, rdd):
    print("----------- %s -----------" % str(time))
    try:
        # convert the RDD to Row RDD
        row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))
        # for row in row_rdd:
        #     df[row['hashtag']] += row['hashtag_count']
        rdd.foreach(update_df)
        print(df)
        # call this method to prepare top 10 hashtags DF and send them
        df.to_csv('hashtag_data.csv')
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)


if __name__ == '__main__':
    # create spark configuration
    conf = SparkConf()
    conf.setAppName("TwitterStreamApp")
    # create spark context with the above configuration
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    # create the Streaming Context from spark context, interval size 2 seconds
    ssc = StreamingContext(sc, 2)
    # setting a checkpoint for RDD recovery (necessary for updateStateByKey)
    ssc.checkpoint("checkpoint_TwitterApp")
    # read data from port 9009
    dataStream = ssc.socketTextStream("twitter", 9009)

    df = pd.DataFrame(g')

    # split each tweet into words
    words = dataStream.flatMap(lambda line: line.split(" "))
    # the tags to monitor and count
    monitored_tags = ['#biden', '#sanders', '#trump', '#warren', '#yang']
    # filter the words to get only hashtags
    hashtags = words.filter(lambda w: w.lower() in monitored_tags)
    # map each hashtag to be a pair of (hashtag,1)
    hashtag_counts = hashtags.map(lambda x: (x.lower(), 1))
    # do the aggregation, note that now this is a sequence of RDDs
    hashtag_totals = hashtag_counts.updateStateByKey(aggregate_tags_count)
    # do this for every single interval
    hashtag_totals.foreachRDD(process_rdd)

    # start the streaming computation
    ssc.start()
    # wait for the streaming to finish
    ssc.awaitTermination()
