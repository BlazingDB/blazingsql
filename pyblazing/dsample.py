import time
import pprint
from blazingsql import BlazingContext
from dask.distributed import Client
# bc = BlazingContext(dask_client=Client('127.0.0.1:8786'), network_interface="lo")
bc = BlazingContext()

dir_data_fs = '/home/aocsa/tpch/100MB2Part/tpch/'
nfiles = 4

# bc.create_table('customer', [dir_data_fs + '/customer_0_0.parquet', dir_data_fs + '/customer_1_0.parquet', dir_data_fs + '/customer_2_0.parquet'])

bc.create_table('customer', dir_data_fs + '/customer_*.parquet')
bc.create_table('nation', dir_data_fs + '/nation_*.parquet')

# "BindableTableScan(table=[[main, customer]], 
# filters=[[OR(AND(<($0, 15000), =($1, 5)), =($0, *($1, $1)), >=($1, 10), <=($2, 500))]], 
# projects=[[0, 3, 5]], aliases=[[c_custkey, c_nationkey, c_acctbal]])"
# query = """select c_custkey, c_nationkey, c_acctbal
#             from 
#               customer
#             where 
#               c_custkey > 2990 and c_custkey < 3010 
#             """

# query = "SELECT sum(c_nationkey) FROM customer group by c_nationkey"
# query = "SELECT c_nationkey FROM customer order by c_nationkey"
# query = "select c_nationkey, c_custkey from customer where c_acctbal < 10000 group by c_nationkey, c_custkey order by c_nationkey desc, c_custkey asc"
query = "select * from customer as c inner join nation as n on c.c_nationkey = n.n_nationkey"


# [b'c_custkey', b'c_name', b'c_address', b'c_nationkey', b'c_phone', b'c_acctbal', b'c_mktsegment', b'c_comment']
lp = bc.explain(query)
print(lp)
ddf = bc.sql(query, use_execution_graph=True)
print(query)
print(ddf)