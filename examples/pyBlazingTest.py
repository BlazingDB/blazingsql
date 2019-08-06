import pyblazing
import pyblazing.apiv2 as blazingsql
# from pyblazing.apiv2 import context
import cudf
from cudf.dataframe import DataFrame
import pandas as pd
import time
import sys

def main():
  
  bc = blazingsql.make_context()

  proofColNames = ['p_artist', 'p_rating', 'p_year', 'p_location', 'p_festival']
  bc.create_table('proof', '/home/user/blazingdb/datasets/proof_header.csv', delimiter=',', skiprows=0, names=proofColNames, nrows=2)

  query = "select * from main.proof"
  result = bc.sql(query).get()
  print(result.columns)


if __name__ == '__main__':
  main()