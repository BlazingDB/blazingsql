import time
import pprint
from blazingsql import BlazingContext
from dask.distributed import Client
bc = BlazingContext(dask_client=Client('127.0.0.1:8786'), network_interface="lo")
# bc = BlazingContext()

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

query = """ select 
                        s.s_acctbal, s.s_name, n.n_name, p.p_partkey, p.p_mfgr, s.s_address, s.s_phone, s.s_comment
                    from 
                        supplier as s 
                    INNER JOIN nation as n ON s.s_nationkey = n.n_nationkey 
                    INNER JOIN partsupp as ps ON s.s_suppkey = ps.ps_suppkey
                    INNER JOIN part as p ON p.p_partkey = ps.ps_partkey 
                    INNER JOIN region as r ON r.r_regionkey = n.n_regionkey
                    where r.r_name = 'EUROPE' and p.p_size = 15
                        and p.p_type like '%BRASS'
                        and ps.ps_supplycost = (
                            select 
                                min(psq.ps_supplycost)
                            from 
                                partsupp as psq
                            INNER JOIN supplier as sq ON sq.s_suppkey = psq.ps_suppkey
                            INNER JOIN nation as nq ON nq.n_nationkey = sq.s_nationkey
                            INNER JOIN region as rq ON rq.r_regionkey = nq.n_regionkey
                            where
                                rq.r_name = 'EUROPE'
                        )
                    order by 
                        s.s_acctbal desc, n.n_name, s.s_name, p.p_partkey"""
lp = bc.explain(query)
print(lp)
ddf = bc.sql(query, use_execution_graph=True)
print(query)
print(ddf)