#!/usr/bin/python3

import sys

old_key = None
business_ids = []

for line in sys.stdin:
    key, value = line.strip().split('\t')

    if key != old_key:
        if old_key:
            print('{}\t{}'.format(old_key, ' ,'.join(business_ids)))
        old_key = key
        business_ids = []
    business_ids.append(value)

print('{}\t{}'.format(old_key, ' ,'.join(business_ids)))