import time
import cudf
import pprint
from blazingsql import BlazingContext
from dask.distributed import Client
import dask_cudf

from collections import OrderedDict 
from blazingsql import BlazingContext
bc = BlazingContext()
# client = Client('127.0.0.1:8786')
# bc = BlazingContext(dask_client=client, network_interface="lo")

customer_gdf = cudf.read_parquet('/home/aocsa/tpch/100MB2Part/tpch/customer_0_0.parquet')

bc.create_table('customer', customer_gdf)
bc.create_table('orders', '/home/aocsa/tpch/100MB2Part/tpch/orders_*.parquet')
bc.create_table('nation', '/home/aocsa/tpch/100MB2Part/tpch/nation_*.parquet')
bc.create_table('region', '/home/aocsa/tpch/100MB2Part/tpch/region_*.parquet')

names = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']
dtype =  ["int64", "int64", "int64", "int32", "float64", "float64", "float64", "float64", "str", "str", "date64", "date64", "date64", "str", "str", "str"]

bc.create_table('lineitem', '/home/aocsa/tpch/100MB2Part/tpch/lineitem_*.psv', delimiter = '|', names=names, dtype=dtype)

# bc.create_table('lineitem', '/home/aocsa/tpch/100MB2Part/tpch/lineitem_*.parquet')

def run_query(bc, sql, title):
    print(title, sql)
    # print(bc.explain(sql))
    result_gdf = bc.sql(sql)
    if isinstance(result_gdf, dask_cudf.core.DataFrame) : 
        print(result_gdf.compute())
    else:
        print(result_gdf)

# distributed sort
queryId = 'TEST_01: distributed sort'
sql = "select c_custkey, c_nationkey from customer order by c_nationkey"
run_query(bc, sql, queryId)

queryId = 'TEST_02: distributed sort'
sql = "select c_custkey, c_acctbal from customer order by c_acctbal desc, c_custkey"
run_query(bc, sql, queryId) 

queryId = 'TEST_03: distributed sort'
sql = "select o_orderkey, o_custkey from orders order by o_orderkey desc, o_custkey"
run_query(bc, sql, queryId) 

# distributed join
queryId = 'TEST_01: distributed join'
sql = "select count(c.c_custkey), sum(c.c_nationkey), n.n_regionkey from customer as c inner join nation as n on c.c_nationkey = n.n_nationkey group by n.n_regionkey"
run_query(bc, sql, queryId)

queryId = 'TEST_02: distributed join'
sql = "select c.c_custkey, c.c_nationkey, n.n_regionkey from customer as c inner join nation as n on c.c_nationkey = n.n_nationkey where n.n_regionkey = 1 and c.c_custkey < 50"
run_query(bc, sql, queryId)

queryId = 'TEST_03: distributed join'
sql = "select c.c_custkey, c.c_nationkey, n.o_orderkey from customer as c inner join orders as n on c.c_custkey = n.o_orderkey where n.o_orderkey < 1000"
run_query(bc, sql, queryId)

# distributed group_by
queryId = 'TEST_01: distributed group_by'
sql = "select count(c_custkey), sum(c_acctbal), sum(c_acctbal)/count(c_acctbal), min(c_custkey), max(c_nationkey), c_nationkey from customer group by c_nationkey"
run_query(bc, sql, queryId)

queryId = 'TEST_02: distributed group_by'
sql = "select count(c_custkey), sum(c_acctbal), sum(c_acctbal)/count(c_acctbal), min(c_custkey), max(c_custkey), c_nationkey from customer where c_custkey < 50 group by c_nationkey"
run_query(bc, sql, queryId)

queryId = 'TEST_04: distributed group_by'
sql = "select c_nationkey, count(c_acctbal) from customer group by c_nationkey, c_custkey"
run_query(bc, sql, queryId)

# all queries 
queryId = 'TEST_01: all distributed operations'
sql = """select c.c_custkey, c.c_nationkey, o.o_orderkey from customer as c 
            inner join orders as o on c.c_custkey = o.o_orderkey 
            inner join nation as n on c.c_nationkey = n.n_nationkey 
            order by c_custkey"""
run_query(bc, sql, queryId)


queryId = 'TEST_01: issue'
sql = """select l_orderkey, l_partkey, l_suppkey, l_returnflag from lineitem 
                where l_returnflag='N' and l_linenumber < 3 and l_orderkey < 50"""
run_query(bc, sql, queryId)


sql = """(select l_shipdate, l_orderkey, l_linestatus from lineitem where l_linenumber = 1 order by 1,2, 3 limit 10)
                union all
                (select l_shipdate, l_orderkey, l_linestatus from lineitem where l_linenumber = 1 order by 1, 3 desc, 2 limit 10)"""
run_query(bc, sql, "union issue")

sql = "select count(n1.n_nationkey) as n1key, count(n2.n_nationkey) as n2key, count(*) as cstar from nation as n1 full outer join nation as n2 on n1.n_nationkey = n2.n_nationkey + 6"
run_query(bc, sql, "join issue")

sql = """(select l_shipdate, l_orderkey, l_linestatus from lineitem where l_linenumber = 1 order by 1,2, 3 limit 10)
                union all
                (select l_shipdate, l_orderkey, l_linestatus from lineitem where l_linenumber = 1 order by 1, 3 desc, 2 limit 10)"""
run_query(bc, sql, "union issue")



sql = """select l.l_orderkey, l.l_linenumber from lineitem as l 
                        inner join orders as o on l.l_orderkey = o.o_orderkey 
                        and l.l_commitdate < o.o_orderdate 
                        and l.l_receiptdate > o.o_orderdate"""
run_query(bc, sql, "union issue")


sql = "select count(n_nationkey), count(*) from nation group by n_nationkey"
run_query(bc, sql, "count issue")

sql = "select CASE WHEN mod(l_linenumber,  2) <> 1 THEN 0 ELSE l_quantity END as s, l_linenumber, l_quantity from lineitem limit 100"
run_query(bc, sql, "case issue")



sql = """(select l_shipdate, l_orderkey, l_linestatus from lineitem where l_linenumber = 1 order by 1,2, 3 limit 10)
                union all
                (select l_shipdate, l_orderkey, l_linestatus from lineitem where l_linenumber = 1 order by 1, 3 desc, 2 limit 10)"""

run_query(bc, sql, "join issue")