#!/usr/bin/python3
"""
breducer.py
Ken Tjhia 2019291691
hexken@my.yorku.ca
"""
import sys
from itertools import groupby
from operator import itemgetter


# generator for input
def read_mapper_output(f):
    for line in f:
        yield line.split('\t')


# just sum the counts for each key
def main():
    data = read_mapper_output(sys.stdin)
    for current_word, group in groupby(data, itemgetter(0)):
        total_count = sum(int(count) for current_word, count in group)
        print('{}\t{}'.format(current_word, total_count))


if __name__ == '__main__':
    main()
