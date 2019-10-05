import pandas as pd
import matplotlib.pyplot as plt
import argparse

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
restaurantCategoryDist = categories[~f].value_counts()

# print the categories and their counts
print('restaurantCategoryDist:')
print('category:#restaurants:')
for s in restaurantCategoryDist.iteritems():
    print('{}:{:0}'.format(s[0], s[1]))

# create DataFrame to hold review counts and avg stars
#zeroes = restaurantCategoryDist * 0
#restaurantReviewDist = pd.concat([zeroes, zeroes], axis=1)
#restaurantReviewDist.columns= ['review_count', 'avg_stars']
restaurantReviewDist = pd.DataFrame(index=restaurantCategoryDist.index, columns=['review_count', 'avg_stars'])

# fill the DataFrame
i = 0
for c in restaurantCategoryDist.index:
    indexes = categories[(categories == c)].index
    restaurantReviewDist.iloc[i,0] = city_restaurants.loc[indexes]['review_count'].sum()
    restaurantReviewDist.iloc[i,1] = city_restaurants.loc[indexes]['stars'].mean()
    i += 1

# sort descending by review_counts
restaurantReviewDist = restaurantReviewDist.sort_values(by=['review_count'], ascending=False)

# print the review counts and avg stars
print('\nrestaurantReviewDist:')
print('category:#reviews:avg_stars')
for s in restaurantReviewDist.iterrows():
    print('{}:{}:{:0.2f}'.format(s[0], s[1][0], s[1][1]))

# make a horizontal histogram of the top 10 largest categories, descending
top10 = restaurantCategoryDist.iloc[0:10]
plt.rcdefaults()
fig, ax = plt.subplots()
ax.barh(top10.index, top10.values, align='center')
ax.set_yticklabels(top10.index)
ax.invert_yaxis()  # labels read top-to-bottom
ax.set_xlabel('Number of restaurants')
ax.set_title('Number of restaurants per category')
plt.show()

