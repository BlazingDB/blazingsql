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

  countryColNames = ['e_country', 'e_comment', 'e_extension']
  

  # bc.create_table('nation', "/home/william/repos/DataSets/DataSet100Mb/nation_0_0.parquet")
  # # nation_gdf = cudf.read_csv("/home/william/repos/DataSets/DataSet100Mb/nation_0_0.psv", delimiter='|', dtype=nationColTypes, names=nationColNames)
  # # bc.create_table('nation', nation_gdf)
  
  # bc.create_table('lineitem', ["/home/william/repos/DataSets/DataSet100Mb/lineitem_0_0.psv"], delimiter='|', dtype=lineitemColTypes, names=lineitemColNames)
  #lineitem_gdf = cudf.read_csv("/home/william/repos/DataSets/DataSet100Mb/lineitem_0_0.psv", delimiter='|', dtype=lineitemColTypes, names=lineitemColNames)
  #bc.create_table('lineitem', lineitem_gdf)
  # bc.create_table('customer', ["/home/william/repos/DataSets/DataSet100Mb/customer_0_0.psv"], delimiter='|', dtype=customerColTypes, names=customerColNames)
  # bc.create_table('supplier', ["/home/william/repos/DataSets/DataSet100Mb/supplier_0_0.psv"], delimiter='|', dtype=supplierColTypes, names=supplierColNames)

  # bc.create_table('nation', "/home/william/repos/DataSets/DataSet100Mb/nation_0_0.parquet")
  # bc.create_table('lineitem', ["/home/william/repos/DataSets/DataSet100Mb/lineitem_0_0.parquet"])
  # bc.create_table('orders', ["/home/william/repos/DataSets/DataSet100Mb/orders_0_0.parquet"])
  # bc.create_table('customer', ["/home/william/repos/DataSets/DataSet100Mb/customer_0_0.parquet"])
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

  # query = "select l_orderkey, l_suppkey from main.lineitem limit 5"
  #result1 = bc.sql(query).get()
  #print(result1)

  # query = "select count(n_nationkey), count(n_regionkey) from main.nation"
  # result1 = bc.sql(query).get()
  # print(query)
  # print(result1)

  # query = """select n_regionkey, max(n_nationkey), count(n_nationkey) from main.nation group by n_regionkey"""
  # result2 = bc.sql(query).get()
  # print(query)
  # print(result2)

  # Change your paths HERE
  # *_header.psv and *_header.csv files already contain header so the [names] param is not necessary
  bc.create_table('nation', "/home/user/blazingdb/datasets/nation.psv", names=nationColNames)
  bc.create_table('nation_header', "/home/user/blazingdb/datasets/nation_header.psv")
  bc.create_table('region', "/home/user/blazingdb/datasets/region.psv", names=regionColNames)
  bc.create_table('region_header', "/home/user/blazingdb/datasets/region_header.psv")
  bc.create_table('lineitem', "/home/user/blazingdb/datasets/lineitem.psv", names=lineitemColNames)
  bc.create_table('lineitem_header', "/home/user/blazingdb/datasets/lineitem_header.psv")
  bc.create_table('country', "/home/user/blazingdb/datasets/country.csv", names=countryColNames)
  bc.create_table('country_header', "/home/user/blazingdb/datasets/country_header.csv")
  bc.create_table('business_2018', "/home/user/blazingdb/datasets/business-operations-2018.csv")

  # Calling sql() and get() functions
  result_nation = bc.sql("select * from main.nation").get()
  result_nation_header = bc.sql("select * from main.nation_header").get()
  result_region = bc.sql("select * from main.region").get()
  result_region_header = bc.sql("select * from main.region_header").get()
  result_lineitem = bc.sql("select * from main.lineitem").get()
  result_lineitem_header = bc.sql("select * from main.lineitem_header").get()
  result_country_csv = bc.sql("select * from main.country").get()
  result_country_header_csv = bc.sql("select * from main.country_header").get()
  result_business = bc.sql("select * from main.business_2018").get()
  
  # uncomment to see the results
  #'''
  # PSV examples
  gdf_nation = result_nation.columns
  print("\ngdf columns of nation:")
  print(gdf_nation)
  print("\ngdf dtypes of nation:")
  print(gdf_nation.dtypes)

  gdf_nation_header = result_nation_header.columns
  print("\ngdf columns of nation_header:")
  print(gdf_nation_header)
  print("\ngdf dtypes of nation_header:")
  print(gdf_nation_header.dtypes)

  gdf_region = result_region.columns
  print("\ngdf columns of region:")
  print(gdf_region)
  print("\ngdf dtypes of region:")
  print(gdf_region.dtypes)
  
  gdf_region_header = result_region_header.columns
  print("\ngdf columns of region_header:")
  print(gdf_region_header)
  print("\ngdf dtypes of region_header:")
  print(gdf_region_header.dtypes)

  gdf_lineitem = result_lineitem.columns
  print("\ngdf columns of lineitem:")
  print(gdf_lineitem)
  print("\ngdf dtypes of lineitem:")
  print(gdf_lineitem.dtypes)

  gdf_lineitem_header = result_lineitem_header.columns
  print("\ngdf columns of lineitem_header:")
  print(gdf_lineitem_header)
  print("\ngdf dtypes of lineitem_header:")
  print(gdf_lineitem_header.dtypes)
  
  # CSV Examples
  gdf_country_csv = result_country_csv.columns
  print("\ngdf columns of country")
  print(gdf_country_csv)
  print("\ngdf dtypes of country:")
  print(gdf_country_csv.dtypes)

  gdf_country_header_csv = result_country_header_csv.columns
  print("\ngdf columns of country_header_csv:")
  print(gdf_country_header_csv)
  print("\ngdf dtypes of country_header_csv:")
  print(gdf_country_header_csv.dtypes)
  
  gdf_business_2018_csv = result_business.columns
  print("\ngdf columns of business_2018_csv:")
  print(gdf_business_2018_csv)
  print("\ngdf dtypes of business_2018_csv:")
  print(gdf_business_2018_csv.dtypes)
  
if __name__ == '__main__':
  main()