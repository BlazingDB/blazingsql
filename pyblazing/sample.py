import time
import pprint
from blazingsql import BlazingContext
from dask.distributed import Client
client = Client('127.0.0.1:8786')
client.restart()
bc = BlazingContext(dask_client=client, network_interface="lo")
# bc = BlazingContext()

dir_data_fs = '/home/aocsa/tpch/DataSet10Parts'
nfiles = 4

bc.create_table('orders', [dir_data_fs + '/orders_0_0.parquet', dir_data_fs + '/orders_1_0.parquet', dir_data_fs + '/orders_2_0.parquet', dir_data_fs + '/orders_3_0.parquet'])

# distributed sort
query = """select
               *
           from
               orders
            """
ddf = bc.sql(query)
# print(ddf.head())