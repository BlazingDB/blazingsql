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

bc.create_table('orders', [dir_data_fs + '/orders_0_0.parquet'])


query = """select *
            from orders 
            where orders.o_orderkey = 10 or orders.o_custkey < 300
            """

lp = bc.explain(query)
print(lp)
ddf = bc.sql(query)
# print(ddf.head())