import pandas as pd
import argparse
from collections import defaultdict

parser = argparse.ArgumentParser()
parser.add_argument('filename', help='file containing the network')
args = parser.parse_args()

# assume edges are bidirectional and no duplicates, as we made in part (c)
# read line by line, remove newline, split id's, and build a dict with user_id for key and
# the number of rows a user appears in as value
data = defaultdict(int)
with open(args.filename, 'r') as f:
    i = -1
    for i, line in enumerate(f):
        u1, u2 = line.strip().split(' ')
        data[u1] += 1
        data[u2] += 1
    n_edges = i + 1
    n_nodes = len(data)

# put data into pandas series, sort by node degree
data = pd.Series(data)
data = data.sort_values(ascending=False)
avgNodeDegree = data.mean()
# print all the stats
print('#nodes:{} #edges:{}'.format(n_nodes, n_edges))
print('nodeDegreeDist:')
for i in data.iteritems():
    print('{}:{}'.format(i[0], i[1]))
print('avgNodeDegree:{:.2f}'.format(round(avgNodeDegree, 2)))
