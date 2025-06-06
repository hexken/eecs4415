#!/usr/bin/python3
'''
I'll remove all punctuation and dollar and cents symbols. N-grams are ordered, so (w1,w2) != (w2,w1)
unless w1=w2. I'm also removing all numbers.
'''
import sys
import csv
import re
import string

punc_string = string.punctuation + '$' + '¢'
reader = csv.reader(sys.stdin, delimiter=',')

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
    # remove words that are all numbers and hyphens, or that contain non latin characters
    print(tip)
    tip = re.sub(r'\b[^\x00-\x7F]+\b|\b\d+\b', ' ', tip)
    print(tip)
    words = tip.split()
    # words = re.split(r'\s', tip)
    # print every word
    for word in words:
        print('{}\t1'.format(word.lower()))
