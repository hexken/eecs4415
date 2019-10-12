import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('filename', help='file containing the businesses')
parser.add_argument('city', help='the city to compute statistics for')
args = parser.parse_args()

# read in only the necessary columns
df = pd.read_csv(args.filename, usecols=['city', 'review_count', 'categories', 'stars'])
# only keep rows in the city with 'restaurant' in categories string
df = df[df.apply(lambda x: x['city'] == args.city and 'restaurant' in x['categories'].lower(), axis=1)]
# create one DataFrame of all categories, can have duplicate rows
data = dict()


# function to pass to df.apply, will fill up the restaurantCategoryDict
def get_stats(reviews, stars, categories):
    for c in categories:
        if c in data:
            data[c][0] += reviews
            data[c][1] += stars
            data[c][2] += 1
        else:
            data[c] = np.array([reviews, stars, 1])


# fill up the data_dict
df.apply(lambda x: get_stats(x['review_count'], x['stars'], x['categories'].split(';')), axis=1)
# Explicitly remove Food and Restaurant
data.pop('Restaurants', None)
data.pop('Food', None)
# overwrite df with the relevant category information
df = pd.DataFrame.from_dict(data, orient='index', columns=['tot_reviews', 'tot_stars', 'tot_businesses'])
# sort by total businesses and print category:num of businesses
restaurantCategoryDist = df.sort_values(by='tot_businesses', ascending=False)
print('restaurantCategoryDist:')
for s in restaurantCategoryDist.itertuples():
    print('{}:{}'.format(s.Index, int(s.tot_businesses)))

# sort by review counts and print the category:total reviews:avg stars
restaurantReviewDist = df.sort_values(by='tot_reviews', ascending=False)
print('-------------------------------------------------------------------\n'
      'restaurantReviewDist:')
for s in restaurantReviewDist.itertuples():
    print('{}:{}:{:0.2f}'.format(s.Index, int(s.tot_reviews), round(s.tot_reviews / s.tot_businesses, 2)))

# make a horizontal histogram of the top 10 largest categories, descending
top10 = restaurantCategoryDist.iloc[0:10, 2]
plt.rcdefaults()
fig, ax = plt.subplots()
ax.barh(top10.index, top10.values, align='center')
ax.set_yticklabels(top10.index)
ax.invert_yaxis()  # labels read top-to-bottom
ax.set_xlabel('Number of restaurants')
ax.set_title('Number of restaurants per category')
plt.savefig('top10categories.png')
plt.show()
