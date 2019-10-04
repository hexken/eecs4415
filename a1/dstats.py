import pandas as pd
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('filename', help='file containing the businesses')
parser.add_argument('city', help='the city to compute statistics for')
args = parser.parse_args()

data = pd.read_csv(args.filename)
# get DataFrame pertaining to the city
city_data = data[data['city'] == args.city]
numOfBus = len(city_data)
avgStars = city_data['stars'].mean()

# get DataFrame pertaining to businesses in the city with 'restaurant' in category string
city_restaurants = city_data[city_data['categories'].str.contains('restaurant', case=False)]
numOfRestaurants = len(city_restaurants)
avgStarsRestaurants = city_restaurants['stars'].mean()
avgNumOfReviews = city_data['review_count'].mean()
avgNumOfReviewsBus = city_restaurants['review_count'].mean()

print('numOfBus: {}\n'
      'avgStars: {}\n'
      'numOfRestaurants: {}\n'
      'avgStarsRestaurants: {}\n'
      'avgNumOfReviews: {}\n'
      'avgNumOfReviewsBus: {}'
      .format(numOfBus, avgStars, numOfRestaurants, avgStarsRestaurants
              , avgNumOfReviews, avgNumOfReviewsBus))
