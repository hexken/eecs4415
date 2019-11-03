#!/usr/bin/python3
import csv
import sys

def main():
    for row in csv.reader(sys.stdin):
        print('{}\t1'.format(row[0]))

if __name__ == '__main__':
    main()
