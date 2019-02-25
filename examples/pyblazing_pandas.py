import pandas as pd
import cudf
import pyblazing


column_names = ['n_nationkey', 'n_name', 'n_regionkey', 'n_comments']
column_types = ['int32', 'int64', 'int32', 'int64']
nation_gdf = cudf.read_csv("data/nation.psv", delimiter='|',
                           dtype=column_types, names=column_names)


column_names = ['r_regionkey', 'r_name', 'r_comment']
column_types = ['int32', 'int64', 'int64']
region_gdf = cudf.read_csv("data/region.psv", delimiter='|',
                           dtype=column_types, names=column_names)


tables = {'nation': nation_gdf, 'region': region_gdf}
# sql = "select * from main.orders as o inner join main.customer as c on o.o_custkey = c.c_custkey where o_orderkey<5000 order by o_totalprice asc, o_orderpriority desc limit 100"
# sql = "select * from main.customer"
sql = "select n.n_nationkey + 1, r.r_regionkey from main.nation as n left outer join main.region as r on n.n_nationkey = r.r_regionkey where n.n_nationkey < 10"
result_gdf = pyblazing.run_query(sql, tables)

print(sql)
print(result_gdf)

sql = "select n.n_nationkey, COALESCE(r.r_regionkey,-1) from main.nation as n left outer join main.region as r on n.n_nationkey = r.r_regionkey where n.n_nationkey < 10"
result_gdf = pyblazing.run_query(sql, tables)



sql = "select n.n_nationkey, r.r_regionkey, COALESCE(r.r_regionkey, n.n_nationkey) from main.nation as n left outer join main.region as r on n.n_nationkey = r.r_regionkey where n.n_nationkey < 10"
result_gdf = pyblazing.run_query(sql, tables)
print(sql)
print(result_gdf)



sql = "select n.n_nationkey, r.r_regionkey, COALESCE(r.r_regionkey, n.n_nationkey - n.n_nationkey ) from main.nation as n left outer join main.region as r on n.n_nationkey = r.r_regionkey where n.n_nationkey < 10"
result_gdf = pyblazing.run_query(sql, tables)
print(sql)
print(result_gdf)



column_names = ['l_orderkey', 'l_receiptdate']
column_types = ['int64', 'date']
lineitem_gdf = cudf.read_csv("data/lineitem.psv", delimiter='|', dtype=column_types, names=column_names)

print (lineitem_gdf)

tables = {'nation': nation_gdf, 'region': region_gdf, 'lineitem': lineitem_gdf}

sql = "select EXTRACT(YEAR FROM l_receiptdate) from main.lineitem"
result_gdf = pyblazing.run_query(sql, tables)

print(sql)
print(result_gdf)
