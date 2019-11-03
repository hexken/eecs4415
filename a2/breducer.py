#!/usr/bin/python3

import sys

old_key = None
count = 0

for line in sys.stdin:
    key, value = line.split('\t')

    if key != old_key:
        if old_key:
            print('{}\t{}'.format(old_key, count))
        old_key = key
        count = 0
    count = count + int(value)

print('{}\t{}'.format(old_key, count))