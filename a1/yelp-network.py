import pandas as pd
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('filename', help='file containing the users')
args = parser.parse_args()

users = pd.read_csv(args.filename, usecols=['user_id', 'friends'])

# get rid of rows that have no friends
users = users[~users['friends'].str.contains('None')]
# turn friend string into a list of strings
users = pd.concat([users['user_id'], users['friends'].str.split(', ')], axis=1)

# open the file and write the network
with open('yelp-network.txt', 'w') as f:
    for user in users.itertuples():
        for friend_id in user[2]:
            f.write('{} {}\n'.format(user[1], friend_id))
            #remove current user_id from friend_id's friend list
            #users[users['user_id'] == friend_id]['friends'].apply(lambda x: x.remove(user[1]))