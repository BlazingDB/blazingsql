import pyblazing
import pyblazing.apiv2 as blazingsql
# from pyblazing.apiv2 import context
import cudf
from cudf.dataframe import DataFrame
import pandas as pd
import time
import sys

def main():
  
  bc = blazingsql.make_context()  

  customerColNames = ["c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"]
  customerColTypes = ["int32", "str", "str", "int32", "str", "float64", "str", "str"]

  ordersColNames = [ 'o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk',
                    'o_shippriority', 'o_comment']
  ordersColTypes = ["int64", "int32", "str", "float64", "date64", "str", "str", "str", "str"]

  lineitemColNames= ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount',
                     'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct',
                     'l_shipmode', 'l_comment']
  lineitemColTypes = ["int64", "int64", "int32", "int32", "float64", "float64", "float64", "float64", "str", "str", "date64", "date64", "date64", "str", "str", "str"]
  
  nationColNames= ['n_nationkey', 'n_name', 'n_regionkey', 'n_comment']
  nationColTypes= ['int32', 'str', 'int32', 'str']

  regionColNames= ['r_regionkey', 'r_name', 'r_comment']
  regionColTypes= ['int32', 'str', 'str']

  supplierColNames = [ 's_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment']
  supplierColTypes = ["int32", "str", "str", "int32", "str", "float64", "str"]

  # bc.create_table('nation', "/home/william/repos/DataSets/DataSet100Mb/nation_0_0.parquet")
  # bc.create_table('region', "/home/william/repos/DataSets/DataSet100Mb/region_0_0.parquet")
  # # nation_gdf = cudf.read_csv("/home/william/repos/DataSets/DataSet100Mb/nation_0_0.psv", delimiter='|', dtype=nationColTypes, names=nationColNames)
  # # bc.create_table('nation', nation_gdf)

  
  
  # bc.create_table('lineitem', ["/home/william/repos/DataSets/DataSet100Mb/lineitem_0_0.psv"], delimiter='|', dtype=lineitemColTypes, names=lineitemColNames)
  # lineitem_gdf = cudf.read_csv("/home/william/repos/DataSets/DataSet100Mb/lineitem_0_0.psv", delimiter='|', dtype=lineitemColTypes, names=lineitemColNames)

  bc.create_table('lineitem', ["/home/william/repos/DataSets/DataSet100Mb/lineitem_0_0.psv"], delimiter='|', dtype=lineitemColTypes, names=lineitemColNames)
  bc.create_table('orders', ["/home/william/repos/DataSets/DataSet100Mb/orders_0_0.psv"], delimiter='|', dtype=ordersColTypes, names=ordersColNames)
  bc.create_table('customer', ["/home/william/repos/DataSets/DataSet100Mb/customer_0_0.psv"], delimiter='|', dtype=customerColTypes, names=customerColNames)
  bc.create_table('supplier', ["/home/william/repos/DataSets/DataSet100Mb/supplier_0_0.psv"], delimiter='|', dtype=supplierColTypes, names=supplierColNames)


  # bc.create_table('nation', "/home/william/repos/DataSets/DataSet100Mb/nation_0_0.parquet")
  # bc.create_table('lineitem', ["/home/william/repos/DataSets/DataSet100Mb/lineitem_0_0.parquet"])
  # bc.create_table('orders', ["/home/william/repos/DataSets/DataSet100Mb/orders_0_0.parquet"])
  bc.create_table('customer', ["/home/william/repos/DataSets/DataSet100Mb/customer_0_0.parquet"])
  # bc.create_table('supplier', ["/home/william/repos/DataSets/DataSet100Mb/supplier_0_0.parquet"])

  # nation_par = cudf.read_parquet("/home/william/repos/DataSets/DataSet100Mb/nation_0_0.parquet")
  # lineitem_par = cudf.read_parquet("/home/william/repos/DataSets/DataSet100Mb/lineitem_0_0.parquet")
  # orders_par = cudf.read_parquet("/home/william/repos/DataSets/DataSet100Mb/orders_0_0.parquet")
  # customer_par = cudf.read_parquet("/home/william/repos/DataSets/DataSet100Mb/customer_0_0.parquet")
  # supplier_par = cudf.read_parquet("/home/william/repos/DataSets/DataSet100Mb/supplier_0_0.parquet")

  # nation_csv = cudf.read_csv("/home/william/repos/DataSets/DataSet100Mb/nation_0_0.psv", delimiter='|', dtype=nationColTypes, names=nationColNames)
  # lineitem_csv = cudf.read_csv("/home/william/repos/DataSets/DataSet100Mb/lineitem_0_0.psv", delimiter='|', dtype=lineitemColTypes, names=lineitemColNames)
  # orders_csv = cudf.read_csv("/home/william/repos/DataSets/DataSet100Mb/orders_0_0.psv", delimiter='|', dtype=ordersColTypes, names=ordersColNames)
  # customer_csv = cudf.read_csv("/home/william/repos/DataSets/DataSet100Mb/customer_0_0.psv", delimiter='|', dtype=customerColTypes, names=customerColNames)
  # supplier_csv = cudf.read_csv("/home/william/repos/DataSets/DataSet100Mb/supplier_0_0.psv", delimiter='|', dtype=supplierColTypes, names=supplierColNames)

  # print("nation par")
  # print(nation_par.dtypes)
  # print("nation csv")
  # print(nation_csv.dtypes)

  # print("lineitem par")
  # print(lineitem_par.dtypes)
  # print("lineitem csv")
  # print(lineitem_csv.dtypes)

  # print("orders par")
  # print(orders_par.dtypes)
  # print("orders csv")
  # print(orders_csv.dtypes)

  # print("customer par")
  # print(customer_par.dtypes)
  # print("customer csv")
  # print(customer_csv.dtypes)

  # print("orders par")
  # print(orders_par.dtypes)
  # print("orders csv")
  # print(orders_csv.dtypes)


  query0 = """select n1.n_nationkey as supp_nation, n2.n_nationkey as cust_nation, l.l_extendedprice * l.l_discount 
  from main.supplier as s 
  inner join main.lineitem as l on s.s_suppkey = l.l_suppkey 
  inner join main.orders as o on o.o_orderkey = l.l_orderkey 
  inner join main.customer as c on c.c_custkey = o.o_custkey 
  inner join main.nation as n1 on s.s_nationkey = n1.n_nationkey 
  inner join main.nation as n2 on c.c_nationkey = n2.n_nationkey 
  where n1.n_nationkey = 1 and n2.n_nationkey = 2 and o.o_orderkey < 10000"""
  query = """select n1.n_nationkey as supp_nation, n2.n_nationkey as cust_nation, l.l_extendedprice * l.l_discount 
    from main.supplier as s 
    inner join main.lineitem as l on s.s_suppkey = l.l_suppkey 
    inner join main.orders as o on o.o_orderkey = l.l_orderkey 
    inner join main.customer as c on c.c_custkey = o.o_custkey 
    inner join main.nation as n1 on s.s_nationkey = n1.n_nationkey 
    inner join main.nation as n2 on c.c_nationkey = n2.n_nationkey 
    where n1.n_nationkey = 1 and n2.n_nationkey = 2 and o.o_orderkey < 10000"""

  # query = "select l_orderkey, l_suppkey from main.lineitem"
 
  result1 = bc.sql(query).get_all()
  print(result1)


  # query = "select count(n_nationkey), count(n_regionkey) from main.nation"
  # result1 = bc.sql(query).get()
  # print(query)
  # print(result1)

  # query = """select n_regionkey, max(n_nationkey), count(n_nationkey) from main.nation group by n_regionkey"""
  # result2 = bc.sql(query).get()
  # print(query)
  # print(result2)

if __name__ == '__main__':
  main()