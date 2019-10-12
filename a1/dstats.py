import pandas as pd
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('filename', help='file containing the businesses')
parser.add_argument('city', help='the city to compute statistics for')
args = parser.parse_args()

# read all of the relevant columns all at once. I do this because the file is small (~30MB),
# the 'full' dataset is only required to briefly stay in memory.
df = pd.read_csv(args.filename, usecols=['city', 'review_count', 'categories', 'stars'])
# get DataFrame pertaining to the city
df = df[df['city'] == args.city]
# compute the quantities pertaining to ALL of the businesses now
avgNumOfReviews = df['review_count'].mean()
numOfBus = len(df)
avgStars = df['stars'].mean()

# slice df to contain only rowsof  businesses in the city with 'restaurant' in category string
df = df[df['categories'].str.contains('restaurant', case=False)]
# compute quantities pertaining to the restaurants within the city
numOfRestaurants = len(df)
avgStarsRestaurants = df['stars'].mean()
avgNumOfReviewsBus = df['review_count'].mean()

print('numOfBus: {}\n'
      'avgStars: {:.2f}\n'
      'numOfRestaurants: {}\n'
      'avgStarsRestaurants: {:.2f}\n'
      'avgNumOfReviews: {:.2f}\n'
      'avgNumOfReviewsBus: {:.2f}'
      .format(numOfBus, round(avgStars, 2), numOfRestaurants, round(avgStarsRestaurants, 2),
              round(avgNumOfReviews, 2), round(avgNumOfReviewsBus, 2)))