#!/usr/bin/python3

import sys
from itertools import groupby
from operator import itemgetter


def read_mapper_output(f):
    for line in f:
        yield line.split('\t')


def main():
    d = {'0': 'Sun', '1': 'Mon', '2': 'Tue', '3': 'Wed', '4': 'Thu', '5': 'Fri', '6': 'Sat'}
    data = read_mapper_output(sys.stdin)
    for current_word, group in groupby(data, itemgetter(0)):
        total_count = sum(int(count) for current_word, count in group)
        business_id, day = current_word.split()
        print('{} {}\t{}'.format(business_id, d[day], total_count))


if __name__ == '__main__':
    main()
