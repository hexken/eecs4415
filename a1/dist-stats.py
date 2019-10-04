import pandas as pd
import matplotlib.pyplot as plt
import argparse
import re

parser = argparse.ArgumentParser()
parser.add_argument('filename', help='file containing the businesses')
parser.add_argument('city', help='the city to compute statistics for')
args = parser.parse_args()

data = pd.read_csv(args.filename)
# get DataFrame of businesses within the city
city_data = data[data['city'] == args.city]
# get DataFrame of businesses within the city that are restaurants
city_restaurants = city_data[city_data['categories'].str.contains('restaurant', case=False)]

# create one DataFrame of all categories, can have duplicate rows
categories = city_restaurants['categories'].str.split(';').explode()

# remove rows of 'restaurant' or 'food', then count how many rows have each unique value
f = categories.str.contains(pat=r'restaurants*|food', regex=True, case=False)
category_counts = categories[~f].value_counts()

for s in category_counts.iteritems():
    print('{}:{}'.format(s[0], s[1]))
