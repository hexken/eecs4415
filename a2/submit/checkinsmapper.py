#!/usr/bin/python3
'''
The strategy is to output tuples of the form <<id, day_num> count>,
by converting the day strings into integers. This will let
hadoop sort the keys by ids and day of week, then we will convert the day integers back to
strings in the reducer.
'''
import sys
import csv


def read_input(f):
    for line in f:
        yield line


def main():
    reader = read_input(csv.reader(sys.stdin, delimiter=','))
    # next(reader)
    for line in reader:
        # to skip the header, since line[3] is an int everywhere else
        try:
            int(line[3])
        except ValueError:
            continue

        # print business_id day_of_week count
        d = {'Sun': 0, 'Mon': 1, 'Tue': 2, 'Wed': 3, 'Thu': 4, 'Fri': 5, 'Sat': 6}
        print('{} {}\t{}'.format(line[0], d[line[1]], int(line[3])))


if __name__ == '__main__':
    main()
