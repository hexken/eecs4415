"""
driver.py
---------
This is the spark driver for the proof of concept program for the project 'Computing TF-IDF Vectors for Subreddits', by
Ken Tjhia <hexken@my.yorku.ca>
Qijin Xu <jackxu@my.yorku.ca>
Ibrahim Suedan <isuedan@hotmail.com>

This program will process a file where each line is a JSON object representing a reddit comment, as in
the files at
    https://files.pushshift.io/reddit/comments

Run the program with
    python driver.py --filepath=filepath [--nwords int] [--nsubreddits int] [--mindf float] [--minlength int] [--vocabsize int]
where filename should be the name of a file as above (with a .txt extension) without the .txt extension
"""
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import CountVectorizer, CountVectorizerModel, IDF
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover
from transformers import Extractor, Filterer, Cleaner, TopKWords, CosineSimilarity, TopKSubreddits
import argparse
from os.path import basename, normpath

# These are only required for TopKWords, which ideally would be defined inside transformers.py
from pyspark.sql.functions import collect_list, udf, col, lower
from pyspark.sql.types import *
from pyspark.ml import Transformer

parser = argparse.ArgumentParser()
parser.add_argument('--filepath', type=str, help='path of file containing the reddit comments')
parser.add_argument('--minlength', type=int, default=50000,
                    help='minimum length of concatenated comments to keep a subreddit')
parser.add_argument('--nwords', type=int, default=10, help='the number of most representative words we find')
parser.add_argument('--nsubreddits', type=int, default=10, help='the number of most similar subreddits we find')
parser.add_argument('--mindf', type=float, default=1.0,
                    help='minimum document frequency to include a word in vocabulary')
parser.add_argument('--vocabsize', type=int, default=20000, help='max number of words to include in vocabulary')
args = parser.parse_args()

spark = SparkSession \
    .builder \
    .enableHiveSupport() \
    .appName("tfidf-reddit-pipeline") \
    .getOrCreate()

# read the data into to a distributed DF
df = spark.read.json(args.filepath + '.txt')
# set up the ETL pipeline
extractor = Extractor(key='subreddit', val='body', inputCol='subreddit', outputCol='body')
cleaner = Cleaner(key='subreddit', val='body', inputCol=extractor.getOutputCol(), outputCol='body')
filterer = Filterer(key='subreddit', val='body', inputCol='subreddit', outputCol='body', minlength=args.minlength)
tokenizer = RegexTokenizer(inputCol=cleaner.getOutputCol(), outputCol="tokens", pattern="\\W")
remover = StopWordsRemover(inputCol=tokenizer.getOutputCol(), outputCol="swr_tokens")
cv = CountVectorizer(inputCol=remover.getOutputCol(), outputCol="tf", minDF=args.mindf, vocabSize=args.vocabsize)
idf = IDF(inputCol=cv.getOutputCol(), outputCol="tfidf")
topkwords = TopKWords(inputCol=idf.getOutputCol(), outputCol='top_words', nwords=args.nwords)
cos_similarity = CosineSimilarity(inputCol='subreddit', outputCol='norm', spark=spark)
topksubreddits = TopKSubreddits(inputCol=cos_similarity.getOutputCol(), outputCol='top_subreddits',
                                nsubreddits=args.nsubreddits)

pipeline = Pipeline(stages=[extractor, cleaner, filterer, tokenizer, remover, cv, idf, topkwords \
    , cos_similarity, topksubreddits])

# fit the model
model = pipeline.fit(df)
stages = model.stages
# extract the vocabulary, pass it to topkwords
vectorizers = [s for s in stages if isinstance(s, CountVectorizerModel)]
topkwords.setVocab(vectorizers[0].vocabulary)
df = model.transform(df)
# drop some unnessecary columns
df = df.drop('body', 'cleaned', 'tf', 'tokens', 'swr_tokens', 'norm')

# save the results to a HIVE table
trans_table = str.maketrans('', '', '-_')
filename = basename(normpath(args.filepath))
df.write.mode('overwrite').saveAsTable(filename.translate(trans_table))
