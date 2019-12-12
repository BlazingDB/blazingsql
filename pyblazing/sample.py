import time
import pprint
from blazingsql import BlazingContext
# from dask.distributed import Client
# client = Client('127.0.0.1:8786')
# client
# client.restart()
bc = BlazingContext()
print('*** Register a POSIX File System ***')
dir_data_fs = '/home/aocsa/tpch/DataSet1GB'
nfiles = 4

bc.create_table('nation', [dir_data_fs + '/nation_0_0.parquet'])

# distributed sort
queryId = 'TEST_01: groupby'
query = """ select
               *
           from
               nation
            """
bc.sql(query)