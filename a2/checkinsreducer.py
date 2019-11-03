#!/usr/bin/python3

import sys

old_key = None
count = 0

d = {'0': 'Sun', '1': 'Mon', '2': 'Tue', '3': 'Wed', '4': 'Thu', '5': 'Fri', '6': 'Sat'}
for line in sys.stdin:
    key, value = line.split('\t')

    if key != old_key:
        if old_key:
            # convert the day integers back into strings before printing
            business_id, day = old_key.split(' ')
            print('{} {}\t{}'.format(business_id, d[day], count))
        old_key = key
        count = 0
    count = count + int(value)

business_id, day = old_key.split(' ')
print('{} {}\t{}'.format(business_id, d[day], count))
