import numpy as np
import pandas as pd
import cudf as cudf
import pyblazing
from pyblazing import SchemaFrom, gdf_dtype, get_dtype_values
from blazingsql import BlazingContext
  

def main():
    bc = BlazingContext()
    # Retrying connect to server: "localhost:53410". Already tried 4 time(s)

    bc.hdfs("localhost", host="localhost", port=54310, user='hadoop')

    dir_data_file = "hdfs://localhost/tpch/"
    names = ['n_nationkey', 'n_name', 'n_regionkey', 'n_comment']
    column_types = ['int', 'long', 'int', 'long']
    dtypes = get_dtype_values(column_types)
    bc.create_table('nation_csv', dir_data_file + 'nation.psv', delimiter='|', dtype=dtypes, names=names)

    sql = 'select n_nationkey, n_regionkey + n_nationkey as addition from main.nation_csv'

    print(sql)
    result = bc.sql(sql, ['nation_csv']).get()
    gdf = result.columns
    print(gdf)

if __name__ == '__main__':
    main()