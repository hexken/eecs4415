import pandas as pd
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('filename', help='file containing the users')
args = parser.parse_args()

# only read in the required columns. I read all of the data initially but the memory
# occupied get reduced very quickly
users = pd.read_csv(args.filename, usecols=['user_id', 'friends'])

# get rid of rows that have no friends
users = users[~users['friends'].str.contains('None')]
# turn friends string into a dict, with friends as keys
users['friends'] = users.apply(lambda x: dict.fromkeys(x['friends'].split(', '), 0), axis=1)
# store user_id and dict of friends in another dict
friends_dict = pd.Series(users['friends'].values, index=users['user_id']).to_dict()
# open the file and write the network
with open('yelp-network.txt', 'w') as f:
    for user in friends_dict.items():
        for friend_id in user[1].keys():
            f.write('{} {}\n'.format(user[0], friend_id))
            # remove current user_id from friend_id's dict
            if friend_id in friends_dict:
                friends_dict[friend_id].pop(user[0], None)
