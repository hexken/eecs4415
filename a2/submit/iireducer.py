#!/usr/bin/python3

import sys
from itertools import groupby
from operator import itemgetter


def read_mapper_output(f):
    for line in f:
        yield line.rstrip().split('\t')


def main():
    data = read_mapper_output(sys.stdin)
    for key, group in groupby(data, itemgetter(0)):
        ids = [g[1] for g in group]
        print('{}\t{}'.format(key, ', '.join(ids)))


if __name__ == '__main__':
    main()
