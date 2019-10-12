import pandas as pd
import matplotlib.pyplot as plt
import argparse

# expects name of file containing businesses info, and city to query
parser = argparse.ArgumentParser()
parser.add_argument('filename', help='file containing the businesses')
parser.add_argument('city', help='the city to compute statistics for')
args = parser.parse_args()

# read in only the necessary columns
df = pd.read_csv(args.filename, usecols=['city', 'review_count', 'categories', 'stars'])
# only keep rows in the city with 'restaurant' in categories string
df = df[df.apply(lambda x: x['city'] == args.city and 'restaurant' in x['categories'].lower(), axis=1)]

# create one DataFrame of all categories, can have duplicate rows
categories = df['categories'].str.split(';').explode()

# remove rows of 'restaurant' or 'food', then count how many rows have each unique value
f = categories.str.contains(pat=r'restaurants*|food', regex=True, case=False)
restaurantCategoryDist = categories[~f].value_counts()

# print the categories and their counts
print('restaurantCategoryDist:')
for s in restaurantCategoryDist.iteritems():
    print('{}:{:0}'.format(s[0], s[1]))

# create DataFrame to hold review counts and avg stars
restaurantReviewDist = pd.DataFrame(index=restaurantCategoryDist.index, columns=['review_count', 'avg_stars'])

# fill the DataFrame
i = 0
for c in restaurantCategoryDist.index:
    indexes = categories[(categories == c)].index
    restaurantReviewDist.iloc[i, 0] = df.loc[indexes]['review_count'].sum()
    restaurantReviewDist.iloc[i, 1] = df.loc[indexes]['stars'].mean()
    i += 1

# sort descending by review_counts
restaurantReviewDist = restaurantReviewDist.sort_values(by=['review_count'], ascending=False)

# print the review counts and avg stars
print('-------------------------------------------------------------------\n'
      'restaurantReviewDist:')
for s in restaurantReviewDist.itertuples():
    print('{}:{}:{:0.2f}'.format(s[0], s[1], s[2]))

# make a horizontal histogram of the top 10 largest categories, descending
top10 = restaurantCategoryDist.iloc[0:10]
plt.rcdefaults()
fig, ax = plt.subplots()
ax.barh(top10.index, top10.values, align='center')
ax.set_yticklabels(top10.index)
ax.invert_yaxis()  # labels read top-to-bottom
ax.set_xlabel('Number of restaurants')
ax.set_title('Number of restaurants per category')
plt.savefig('top10categories.png')
plt.show()
