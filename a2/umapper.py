#!/usr/bin/python3
"""
umapper.py
Ken Tjhia 2019291691
hexken@my.yorku.ca

I'll remove all punctuation and dollar and cents symbols. N-grams are ordered, so (w1,w2) != (w2,w1)
unless w1=w2. I'm considering a word as a block of one or more English alphabet characters which may contain
numbers.
"""

import sys
import csv
import re
import string


# generator for input
def read_input(f):
    for line in f:
        yield line


def main():
    punc_string = string.punctuation + '$' + '¢'
    reader = read_input(csv.reader(sys.stdin, delimiter=','))

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

        for word in words:
            print('{}\t1'.format(word))


if __name__ == '__main__':
    main()
