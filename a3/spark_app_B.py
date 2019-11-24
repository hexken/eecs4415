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

    Hutto, C.J. & Gilbert, E.E. (2014). VADER: A Parsimonious Rule-based Model for Sentiment Analysis of Social Media Text. Eighth International Conference on Weblogs and Social Media (ICWSM-14). Ann Arbor, MI, June 2014.
"""

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark import Row, SQLContext
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.tokenize import TweetTokenizer
import traceback
import nltk
import requests
import sys

nltk.download('vader_lexicon')
analyzer = SentimentIntensityAnalyzer()
# the topics
topics = ['Drake', 'Katy Perry', 'Taylor Swift', 'Beyonce', 'Rihanna']
# the set of hashtags to monitor for each topic
drake_tags = ['#drake', '#drizzy', '#teamdrizzy', '#drakequotes', '#teamdrake', '#ovo',
              '#champagnepapi']  # , '#', '#', '#'],\
katyperry_tags = ['#katyperry', '#katycats'],  # , '#', '#', '#', '#', '#', '#', '#', '#']\
taylorswift_tags = ['#taylorswift', '#tswift', '#swifties', '#taylorswift13', '#artistofthedecade',
                    '#taylornation', '#taylornation13', '#tayloronamas', '#istandwithtaylor', '#ts1989']
beyonce_tags = ['#beyonce', '#beyhive']  # , '#', '#', '#', '#', '#', '#', '#', '#']
rihanna_tags = ['#rihanna', '#riri', '#fenty', '#fentybeauty']  # , '#', '#', '#', '#', '#', '#']
topic_tags = [drake_tags, katyperry_tags, taylorswift_tags, beyonce_tags, rihanna_tags]


# monitored_tags = dr_tags + kp_tags + ts_tags + be_tags + ri_tags

# accumulate the scores and record the number of tweets (for averaging)


def aggregate_topic_scores(new_values, old_value):
    if old_value is None:
        old_value = (0, 0)
    accum_score = sum(v[0] for v in new_values) + old_value[0]
    num_of_tweets = sum(v[1] for v in new_values) + old_value[1]
    return accum_score, num_of_tweets


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
        row_rdd = rdd.map(lambda w: Row(topic=w[0], accum_score=w[1][0], num_of_tweets=w[1][1]))
        # create a DF from the Row RDD
        topic_accum_scores_df = sql_context.createDataFrame(row_rdd)
        topic_accum_scores_df.show()
        # Register the dataframe as table
        topic_accum_scores_df.registerTempTable("topic_scores")
        # compute the average sentiment (vader compound) for each topic
        topic_scores_df = sql_context.sql(
            "select topic, coalesce(accum_score / num_of_tweets, 0) as score from topic_scores")
        topic_scores_df.show()
        # call this method to prepare top 10 hashtags DF and send them
        send_df_to_dashboard(topic_scores_df)
    except:
        print(traceback.format_exc())
        # e = sys.exc_info()[0]
        # print("Error: %s" % e)


def send_df_to_dashboard(df):
    # extract the hashtags from dataframe and convert them into array
    topic_names = [str(t.topic) for t in df.select("topic").collect()]
    # extract the counts from dataframe and convert them into array
    topic_scores = [p.score for p in df.select("score").collect()]
    # initialize and send the data through REST API
    url = 'http://localhost:5001/updateData'
    request_data = {'label': str(topic_names), 'data': str(topic_scores)}
    response = requests.post(url, data=request_data)


# assign topics to a tweet. If the tweet corresponds to some topics
# then compute the sentiment and return a list of (topic, sentiment) pairs
def tweet_scorer(tokenized_tweet):
    tweet_topics = []
    for w in tokenized_tweet:
        w = w.lower()
        for i in range(len(topics)):
            if w in topic_tags[i]:
                tweet_topics.append(topics[i])

    if len(tweet_topics) == 0:
        return []

    score = analyzer.polarity_scores(''.join(tokenized_tweet))['compound']
    return [(topic, (score, 1)) for topic in tweet_topics]


if __name__ == '__main__':
    try:
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

        # collect tweets related to each topic
        tokenizer = TweetTokenizer(preserve_case=True, reduce_len=False, strip_handles=False)
        # set up initial rdd of (topic (0, 0)) and stream so we can nicely display
        # tables/dashboards with all topics populated even when we haven't seen tweets
        # corresponding to them.
        initial_rdd = [(topic, (0, 0)) for topic in topics]
        initial_rdd = [sc.parallelize(initial_rdd)]
        initial_stream = ssc.queueStream(initial_rdd)

        # split each tweet into a list of words using TweetTokenizer
        tokenized_tweets = dataStream.map(tokenizer.tokenize)
        # map each tokenized tweet to a list of (topic, score) for each
        # topic the tweet corresponds to.
        scores = tokenized_tweets.flatMap(tweet_scorer)
        scores = scores.union(initial_stream)
        # accumulate all of the scores and # of tweets for each topic
        topic_accum_scores = scores.updateStateByKey(aggregate_topic_scores)
        # do processing for each RDD generated in each interval
        topic_accum_scores.foreachRDD(process_rdd)
        # start the streaming computation
        ssc.start()
        # wait for the streaming to finish
        ssc.awaitTermination()
    except:
        print(traceback.format_exc())
