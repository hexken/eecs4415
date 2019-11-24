"""
    spark_app_A.py
    ----------------------------------------------------------------------
    This Spark app connects to a script running on another (Docker) machine
    on port 9009 that provides a stream of raw tweets text, filteres for a particular set of hashtags.
    That stream is meant to be read and processed here, where we count the number of occurrences of
    a particular set of hashtags, display to stdout and send the dataframe to a dashboard app.
    The Twitter and Spark apps are meant to be run in Docker containers.

    To execute this in a Docker container, do:
    
        docker run -it -v $PWD:/app --link twitter:twitter eecsyorku/eecs4415

    and inside the docker:

        spark-submit spark_app.py


    Made for: EECS 4415 - Big Data Systems (York University EECS dept.)
    Modified by: Tilemachos Pechlivanoglou
    Based on: https://www.toptal.com/apache/apache-spark-streaming-twitter
    Original author: Hanee' Medhat

    Further modified by: Ken Tjhia (minor changes)
    For: EECS 4415 Big Data Systems Assignment #3, Part A
"""

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark import Row, SQLContext
import requests
import sys


# adding the count of each hashtag to its last count
def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)


def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']


def process_rdd(time, rdd):
    print("----------- %s -----------" % str(time))
    try:
        # Get spark sql singleton context from the current context
        sql_context = get_sql_context_instance(rdd.context)
        # convert the RDD to Row RDD
        row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))
        # create a DF from the Row RDD
        hashtags_df = sql_context.createDataFrame(row_rdd)
        # Register the dataframe as table
        hashtags_df.registerTempTable("hashtags")
        # sort the hashtags by descening number of occerences
        hashtag_counts_sorted_df = sql_context.sql(
            "select hashtag, hashtag_count from hashtags order by hashtag_count desc")
        hashtag_counts_sorted_df.show()
        send_df_to_dashboard(hashtag_counts_sorted_df)
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)


def send_df_to_dashboard(df):
    # extract the hashtags from dataframe and convert them into array
    tags = [str(t.hashtag) for t in df.select("hashtag").collect()]
    # extract the counts from dataframe and convert them into array
    tags_count = [p.hashtag_count for p in df.select("hashtag_count").collect()]
    # initialize and send the data through REST API
    url = 'http://localhost:5001/updateData'
    request_data = {'label': str(tags), 'data': str(tags_count)}
    response = requests.post(url, data=request_data)


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

    # the hashtags we are going to count occurrences of
    monitored_tags = ['#biden', '#sanders', '#trump', '#warren', '#yang']
    # set up initial rdd of (tag, 0) and stream so we can nicely display 0 counts for
    # tags which have not yet occurred.
    initial_rdd = [(tag, 0) for tag in monitored_tags]
    initial_rdd = [sc.parallelize(initial_rdd)]
    initial_stream = ssc.queueStream(initial_rdd)

    # split each tweet into words
    words = dataStream.flatMap(lambda line: line.split(" "))
    # filter the words to get only hashtags, then map each hashtag to be a pair of (hashtag,1)
    hashtags = words.filter(lambda w: w.lower() in monitored_tags).map(lambda x: (x.lower(), 1))
    hashtags = hashtags.union(initial_stream)
    # adding the count of each hashtag to its last count
    tags_totals = hashtags.updateStateByKey(aggregate_tags_count)
    # do processing for each RDD generated in each interval
    tags_totals.foreachRDD(process_rdd)
    # start the streaming computation
    ssc.start()
    # wait for the streaming to finish
    ssc.awaitTermination()
