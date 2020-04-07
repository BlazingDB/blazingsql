import time
import pprint
from blazingsql import BlazingContext
from dask.distributed import Client
# bc = BlazingContext(dask_client=Client('127.0.0.1:8786'), network_interface="lo")
bc = BlazingContext()

dir_data_fs = '/home/aocsa/tpch/DataSet5Part100MB/'
dir_data_fs = '/home/aocsa/tpch/100MB2Part/tpch/'
nfiles = 4

# bc.create_table('customer', [dir_data_fs + '/customer_0_0.parquet', dir_data_fs + '/customer_1_0.parquet', dir_data_fs + '/customer_2_0.parquet'])

bc.create_table('customer', dir_data_fs + '/customer_*.parquet')
bc.create_table('nation', dir_data_fs + '/nation_*.parquet')
bc.create_table('region', dir_data_fs + '/region_*.parquet')
bc.create_table('orders', dir_data_fs + '/orders_*.parquet')
bc.create_table('lineitem', dir_data_fs + '/lineitem_*.parquet')
bc.create_table('part', dir_data_fs + '/part_*.parquet')
bc.create_table('partsupp', dir_data_fs + '/partsupp_*.parquet')
bc.create_table('supplier', dir_data_fs + '/supplier_*.parquet')


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
# query = "select * from customer as c inner join nation as n on c.c_nationkey = n.n_nationkey and c.c_custkey < 10"

# query = """select c.c_custkey, c.c_nationkey, o.o_orderkey from customer as c 
#             inner join orders as o on c.c_custkey = o.o_orderkey 
#             inner join nation as n on c.c_nationkey = n.n_nationkey 
#             order by c_custkey
# """
# query = "select nation.n_nationkey, region.r_regionkey from nation inner join region on region.r_regionkey = nation.n_nationkey"
# query = "select count(p_partkey) + sum(p_partkey), (max(p_partkey) + min(p_partkey))/2 from part where p_partkey < 100"

# query = "select count(n1.n_nationkey) as n1key, count(n2.n_nationkey) as n2key, count(*) as cstar from nation as n1 full outer join nation as n2 on n1.n_nationkey = n2.n_nationkey + 6"

# query = "select count(c.c_custkey), sum(c.c_nationkey), n.n_regionkey from customer as c inner join nation as n on c.c_nationkey = n.n_nationkey group by n.n_regionkey"

# # TODO:
# query = "select count(n1.n_nationkey) as n1key, count(n2.n_nationkey) as n2key, count(*) as cstar from nation as n1 full outer join nation as n2 on n1.n_nationkey = n2.n_nationkey + 6"
# query = """select c.c_custkey, c.c_nationkey, o.o_orderkey from customer as c 
#         inner join orders as o on c.c_custkey = o.o_custkey 
#         inner join nation as n on c.c_nationkey = n.n_nationkey order by c_custkey, o.o_orderkey"""

query = """select partSuppTemp.partKey, partAnalysis.avgSize from
                (select min(p_partkey) as partKey, avg(p_size) as avgSize, max(p_retailprice) as maxPrice, min(p_retailprice) as minPrice from part ) as partAnalysis
                inner join (select ps_partkey as partKey, ps_suppkey as suppKey from partsupp where ps_availqty > 2) as partSuppTemp on partAnalysis.partKey = partSuppTemp.partKey
                inner join (select s_suppkey as suppKey from supplier ) as supplierTemp on supplierTemp.suppKey = partSuppTemp.suppKey"""

lp = bc.explain(query)
print(lp)
ddf = bc.sql(query, use_execution_graph=True)
print(query)
print(ddf)