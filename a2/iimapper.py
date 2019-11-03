#!/usr/bin/python3

import sys
import csv
import re
import string

reader = csv.reader(sys.stdin, delimiter=',')
# next(reader)
for line in reader:
    # to skip the header, since line[1] is an int everywhere else
    try:
        int(line[11])
    except ValueError:
        continue

    # get the category list and business_id
    categories = line[12].lower()
    categories = categories.split(';')
    business_id = line[0]
    for c in categories:
        print('{}\t{}'.format(c, business_id))
