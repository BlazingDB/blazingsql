import numpy as np
import pandas as pd
import cudf as cudf
import pyblazing
from pyblazing import SchemaFrom
from libgdf_cffi import libgdf
from blazingsql import BlazingContext
  

def get_dtype_values(dtypes):
    values = []
    def gdf_type(type_name):
        dicc = {
            'str': libgdf.GDF_STRING,
            'date': libgdf.GDF_DATE64,
            'date64': libgdf.GDF_DATE64,
            'date32': libgdf.GDF_DATE32,
            'timestamp': libgdf.GDF_TIMESTAMP,
            'category': libgdf.GDF_CATEGORY,
            'float': libgdf.GDF_FLOAT32,
            'double': libgdf.GDF_FLOAT64,
            'float32': libgdf.GDF_FLOAT32,
            'float64': libgdf.GDF_FLOAT64,
            'short': libgdf.GDF_INT16,
            'long': libgdf.GDF_INT64,
            'int': libgdf.GDF_INT32,
            'int32': libgdf.GDF_INT32,
            'int64': libgdf.GDF_INT64,
        }
        if dicc.get(type_name):
            return dicc[type_name]
        return libgdf.GDF_INT64

    for key in dtypes:
        values.append( gdf_type(key))
    
    return values

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