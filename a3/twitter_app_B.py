"""
    twitter_app_B.py
    ----------
    This app sets up a twitter stream that tracks tweets containing 50 hashtags (10 per topic, 5 topic)
    and send the tweets to a Spark app on port 9009 where they are to be processed by a Spark app.

    To execute this in a Docker container, do:

        docker run -it -v $PWD:/app --name twitter -p 9009:9009 python bash

    and inside the docker:

    pip install -U git+https://github.com/tweepy/tweepy.git
        python twitter_app.py

    Made for: EECS 4415 - Big Data Systems (York University EECS dept.)
    Modified by: Tilemachos Pechlivanoglou
    Based on: https://www.toptal.com/apache/apache-spark-streaming-twitter
    Original author: Hanee' Medhat

    Further modified by: Ken Tjhia (minor changes)
    For: EECS 4415 Big Data Systems Assignment #3, Part B
"""
import tweepy
import json
import sys
import socket

# my credentials
consumer_key = "J4ru00YugoaoNN6lbM6oNSLNh"
consumer_secret = "wKUAHw1sMWf2Tcq7Oqqgss82xYEfMFeWTLqaUzgxw1MTLl8Cwq"
access_token = "780411082540195840-luqwRvt2WP4caindy0vapZhZIvi8M7J"
access_token_secret = "l6Y0tDpdf29MRJMXlXPSyWdjIQhy3da3rKYNrxRSJGRMq"

# # the set of hashtags to monitor for each topic
# drake_tags = ['#drake', '#drizzy', '#teamdrizzy', '#drakequotes', '#teamdrake', '#ovo',
#               '#champagnepapi']  # , '#', '#', '#'],\
# katyperry_tags = ['#katyperry', '#katycats'],  # , '#', '#', '#', '#', '#', '#', '#', '#']\
# taylorswift_tags = ['#taylorswift', '#tswift', '#swifties', '#taylorswift13', '#artistofthedecade',
#                     '#taylornation', '#taylornation13', '#tayloronamas', '#istandwithtaylor', '#ts1989']
# beyonce_tags = ['#beyonce', '#beyhive']  # , '#', '#', '#', '#', '#', '#', '#', '#']
# rihanna_tags = ['#rihanna', '#riri', '#fenty', '#fentybeauty']  # , '#', '#', '#', '#', '#', '#']
# topic_tags = [drake_tags, katyperry_tags, taylorswift_tags, beyonce_tags, rihanna_tags]

# the set of hashtags to monitor for each topic
trump_tags = ['#trump', '#trump2020', '#donaldtrump', '#donaldjtrump', '#trumpadministration',
              '#trumpsupporters', '#impeachtrump', '#impeachmenthearings', '#impeachmentinquiery',
              '#trump2020landslide']
sanders_tags = ['#sanders', '#berniesanders', '#sanders2020', '#bernie', '#feelthebern',
                '#votebernie', '#bernieforpresident', '#onlybernie', '#bernieorbust', '#berniesandersyoubastards']
yang_tags = ['#yang', '#andrewyang', '#yanggang', '#yang2020', '#andrewyang2020',
             '#yangforpresident', '#yangmediablackout', '#msnbcfearsyang', '#yanggang2020', '#yangthegoat']
warren_tags = ['#warren', '#warren2020', '#elizabethwarren', '#warrenforpresident', '#teamwarren',
               '#senwarren', '#warrenmemeteam', '#ewarren', '#elizabethwarren2020', '#pdx4warren']
biden_tags = ['#biden', '#biden2020', '#joebiden', '#bidenforpresident', '#joebiden2020',
              '#teambiden', '#teamjoe', '#joe2020', '#bidencrimefamily', '#joe_biden']
topic_tags = trump_tags + sanders_tags + yang_tags + warren_tags + biden_tags


class TweetListener(tweepy.StreamListener):
    """
    Listener that will send tweets to a spar
    """

    def on_error(self, status):
        if status == 420:
            return False

    # def on_status(self, status):
    #    print(status)

    def on_data(self, data):
        """When a tweet is received, forward it"""
        try:
            global conn

            # load the tweet JSON, get pure text
            full_tweet = json.loads(data)
            tweet_text = full_tweet['text'] + '\n'

            # print the tweet plus a separator
            print("------------------------------------------")
            print(tweet_text)

            # send it to spark
            conn.send(tweet_text.encode('utf-8'))
        except:
            # handle errors
            e = sys.exc_info()[0]
            print("Error: %s" % e)

        return True


class HashTagStream():

    def __init__(self, auth, listener):
        # setup search terms
        self.track = topic_tags
        self.stream = tweepy.Stream(auth, listener)

    def start(self):
        try:
            self.stream.filter(track=self.track, is_async=True)
        except KeyboardInterrupt:
            print('exiting')
        s.shutdown(socket.SHUT_RD)


if __name__ == '__main__':
    # IP and port of local machine or Docker
    TCP_IP = socket.gethostbyname(socket.gethostname())  # returns local IP
    TCP_PORT = 9009

    # setup local connection, expose socket, listen for spark app
    conn = None
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((TCP_IP, TCP_PORT))
    s.listen(1)
    print("Waiting for TCP connection...")

    # if the connection is accepted, proceed
    conn, addr = s.accept()
    print("Connected... Starting getting tweets.")

    # authenticate twitter
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    # set up the listener and stream
    listener = TweetListener()
    stream = HashTagStream(auth, listener)
    stream.start()
