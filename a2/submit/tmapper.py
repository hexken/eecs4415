#!/usr/bin/python3

import sys
import csv
import re
import string

punc_string = string.punctuation + '$' + '¢'
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
    # remove blocks of characters which are all numbers or those that contain non English alphabet
    # characters
    words = re.sub(r'\s*\w*[^\x00-\x7F]+\w*\s*|\b\d*\b', ' ', tip).split()
    
    for i in range(len(words) - 2):
        print('{} {} {}\t1'.format(words[i], words[i + 1], words[i + 2]))
