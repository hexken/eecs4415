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
# store user_id and list of friends in dict
friends_dict = pd.Series(users['friends'].values, index=users['user_id']).to_dict()
# open the file and write the network
with open('yelp-network.txt', 'w') as f:
    for user in friends_dict.items():
        for friend_id in user[1]:
            f.write('{} {}\n'.format(user[0], friend_id))
            # remove current user_id from friend_id's friend list
            if friend_id in friends_dict:
                # sometimes the friend relation is not stored bidrectionally..
                if user[0] in friends_dict[friend_id]:
                    friends_dict[friend_id].remove(user[0])