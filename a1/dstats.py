import pandas as pd
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('filename', help='file containing the businesses')
parser.add_argument('city', help='the city to compute statistics for')
args = parser.parse_args()

data = pd.read_csv(args.filename)
city = args.city


