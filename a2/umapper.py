#!/usr/bin/python3

import sys
import csv
import re
import string

punc_string = string.punctuation.replace('-', '') + '$' + '¢'
reader = csv.reader(sys.stdin, delimiter=',')
# next(reader)
for line in reader:
    # to skip the header, since line[2] is an int everywhere else
    try:
        int(line[2])
    except ValueError:
        continue

    # get the tip string
    tip = line[0]
    # so we can remove all periods without collapsing words like word1...word2
    tip = tip.replace('.', ' ')
    # remove all punctuation
    tip = tip.translate(tip.maketrans('', '', punc_string)).lower()
    # remove words that are all numbers and hyphens
    words = re.sub(r'([0-9-]+)', '', tip).split()
    # print every word
    for word in words:
        print('{}\t1'.format(word))
