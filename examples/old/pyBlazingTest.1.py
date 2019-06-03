import pyblazing.apiv2 as blazingsql
# from pyblazing.apiv2 import context
import cudf
from cudf.dataframe import DataFrame
import pandas as pd
import time
import sys



def main():
  
  bc = blazingsql.make_context()  

  column_names = ["c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"]
  column_types = ["int32", "str", "str", "int32", "str", "float64", "str", "str"]

  ordersColNames = [ 'o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk',
                    'o_shippriority', 'o_comment']
  ordersColTypes = ["int64", "int64", "str", "float64", "date64", "str", "str", "str", "str"]

  lineitemColNames= ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount',
                     'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct',
                     'l_shipmode', 'l_comment']
  lineitemColTypes = ["int32", "int64", "int64", "int32", "float64", "float64", "float64", "float64", "str", "str", "date64", "date64", "date64", "str", "str", "str"]
  
  nationColNames= ['n_nationkey', 'n_name', 'n_regionkey', 'n_comment']
  nationColTypes= ['int32', 'str', 'int32', 'str']

  regionColNames= ['r_regionkey', 'r_name', 'r_comment']
  regionColTypes= ['int32', 'str', 'str']

  # cust_gdf = cudf.read_csv("/home/william/repos/DataSets/tpch10MbTest/customer_0_0.psv", delimiter='|', dtype=column_types, names=column_names)
  # cust_gdf = cudf.read_csv("/home/william/repos/BlazingEngineIntegrationTests/tpch_2_17_0/dbgen/customer.tbl", delimiter='|', dtype=column_types, names=column_names)
  # bc.create_table('customer', cust_gdf)
  # bc.create_table('customer', "/home/william/repos/DataSets/DataSet100Mb/customer_0_0.psv", delimiter='|', dtype=column_types, names=column_names)
  # # bc.create_table('customer', "/home/william/repos/DataSets/DataSet100Mb/customer_0_0.parquet")

  # for i in range(0,2):
  #   print('interation')
  #   print(i)
  #   result = bc.sql('select * from main.customer',['customer']).get()
  #   print("result")
  #   print(result)
  #   time.sleep(1.5)

  # print("now for CUDF BASED")

  # cust_gdf = cudf.read_csv("/home/william/repos/DataSets/DataSet100Mb/customer_0_0.psv", delimiter='|', dtype=column_types, names=column_names)  

  # bc.create_table('customer2', cust_gdf)
  # print("read the file")
  # # time.sleep(3)

  # for i in range(0,10):
  #   print('interation')
  #   print(i)
  #   query = 'select * from main.customer2'
  #   result = bc.sql(query,['customer2']).get()
  #   print("result")
  #   print(result)
  #   # time.sleep(1.5)

  # print('DONE!!')

  # print("now for CUDF BASED")

  # cust_gdf = cudf.read_csv("/home/william/repos/DataSets/tpch400MbTest/lineitem_0_0.psv", delimiter='|', dtype=lineitemColTypes, names=lineitemColNames)   

  # bc.create_table('lineitem', cust_gdf)
  # # bc.create_table('lineitem', "/home/william/repos/DataSets/tpch400MbTest/lineitem_0_0.psv", delimiter='|', dtype=lineitemColTypes, names=lineitemColNames)
  # print("read the file")
  # # time.sleep(3)

  # # filt = [100, 1000000, 1500000]

  # result0 = bc.sql('select l_orderkey from main.lineitem where l_orderkey > 10',['lineitem']).get()
  # # del result0
  # result1 = bc.sql('select  l_orderkey from main.lineitem where l_orderkey > 10',['lineitem']).get()
  # # # del result0
  # result2 = bc.sql('select  l_orderkey from main.lineitem where l_orderkey > 10',['lineitem']).get()
  # # # del result2
  # # print(result2)

  
  
  # for i in range(0,3):
  # # for i in filt:
  #   print('interation')
  #   print(i)
  #   # cust_gdf = cudf.read_csv("/home/william/repos/DataSets/tpch400MbTest/lineitem_0_0.psv", delimiter='|', dtype=lineitemColTypes, names=lineitemColNames, usecols=[0])  
  #   # result = bc.sql('select l_orderkey from main.lineitem where l_orderkey > ' + str(i),['lineitem']).get()
  #   result = bc.sql('select l_orderkey from main.lineitem where l_orderkey > 10',['lineitem']).get()
  #   # print(type(result))
  #   print(id(result))
  #   # del result
    
  #   # print(result)
  #   # time.sleep(1.5)
  # # # run_queries(bc)

  
  # time.sleep(3)

  # print('DONE!!')

  # nation_gdf = cudf.read_csv("/home/william/repos/DataSets/DataSet100Mb/nation_0_0.psv", delimiter='|', dtype=nationColTypes, names=nationColNames)  
  # bc.create_table('nation', nation_gdf)
  # region_gdf = cudf.read_csv("/home/william/repos/DataSets/DataSet100Mb/region_0_0.psv", delimiter='|', dtype=regionColTypes, names=regionColNames)  
  # bc.create_table('region', region_gdf)
  orders_gdf = cudf.read_csv("/home/william/repos/DataSets/DataSet100Mb/orders_0_0.psv", delimiter='|', dtype=ordersColTypes, names=ordersColNames)  
  # print(orders_gdf)
  bc.create_table('orders', orders_gdf)

  # query0 = """select r_regionkey, r_name from main.region where r_regionkey > 2"""
  # result0 = bc.sql(query0, ['region']).get()
  # print(result0)
  # query1 = """select count(n_nationkey) from main.nation group by n_nationkey"""
  # result1 = bc.sql(query1, ['nation']).get()
  # print(result1)

  # query = """with regionTemp as ( select r_regionkey, r_name from main.region where r_regionkey > 2 ),
  #   nationTemp as (select n_nationkey, n_regionkey as fkey, n_name from main.nation where n_nationkey > 3 order by n_nationkey) 
  #   select regionTemp.r_name, nationTemp.n_name from regionTemp inner join nationTemp on regionTemp.r_regionkey = nationTemp.fkey"""
  # result = bc.sql(query, ['nation', 'region']).get()
  # print(result)

  # result2 = bc.sql("select min(o_orderdate) as miny, max(o_orderdate) as maxy from main.orders where o_orderdate = DATE '1996-01-02'", ['orders']).get()
  # print(result2)

  print(orders_gdf['o_orderdate'].astype('int64'))
  result2 = bc.sql("select o_orderdate from main.orders where o_orderdate = DATE '1996-01-02'", ['orders']).get()
  print(result2)
  
  result3 = bc.sql("select min(o_orderdate) as miny, max(o_orderdate) as maxy from main.orders where o_orderdate < DATE '1995-02-04' and o_orderdate > DATE '1994-09-22'", ['orders']).get()
  print(result3)

  # result4 = bc.sql("select o_orderdate from main.orders", ['orders']).get()
  # print(result4)

  # bc.create_table('orders2', result2.columns)
  # result3 = bc.sql("select EXTRACT(YEAR FROM miny), EXTRACT(MONTH FROM miny), EXTRACT(DAY FROM miny), EXTRACT(YEAR FROM maxy), EXTRACT(MONTH FROM maxy), EXTRACT(DAY FROM maxy) from main.orders2", ['orders2']).get()
  # print(result3)

  # print("read the file")
  # # time.sleep(3)

  # for i in range(0,10):
  #   print('interation')
  #   print(i)
  #   query = 'select * from main.nation where n_nationkey >= ' + str(i)
  #   result = bc.sql(query,['nation']).get()
  #   print("result")
  #   print(result)
    
    

    

  # print('DONE!!')

  

  # bc.create_table('c2', result)
  # result2 = bc.sql('select * from main.c2 where c_custkey < 0 or c_nationkey >= 24',['c2']).get()
  # print("result2")
  # print(result2)




  # print(sql)
  # print(result_gdf)

  # #Define Table
  # column_names = ['key','fare_amount','pickup_longitude', 'dropoff_longitude', 'pickup_latitude', 'dropoff_latitude', 'passenger_count']
  # column_types = ['date64', 'float32', 'float32', 'float32','float32','float32', 'float32']
  # # column_types = ['date64', 'float32', 'int32', 'int32','int32','int32', 'int32']

  # # taxi_gdf = cudf.read_csv('/home/william/repos/DataSets/taxi_training_data.csv', delimiter=',', dtype=column_types, names=column_names)
  
  # # # #Create Table
  # bc.create_table('taxi', '/home/william/repos/DataSets/taxi_training_data.csv', delimiter=',', dtype=column_types, names=column_names)
  # # # # bc.create_table('taxi', taxi_gdf)
  # # # # #Query Table for Training Data
  # X_train = bc.sql('SELECT count(pickup_latitude), count(coalesce(pickup_latitude,0)) as pickup_latitude FROM main.taxi', ['taxi']).get()
  # print(X_train.columns)
  # # bc.create_table('taxi2', X_train)

  # X_train2 = bc.sql('SELECT count(pickup_latitude)  FROM main.taxi2', ['taxi2']).get()
  # # print(X_train2.columns)



  # dataFolder10 = '/home/william/repos/DataSets/tpch10Mb/'
  # # dataFolder100 = '/home/william/repos/DataSets/DataSet100Mb/'
  
  # # parquetFolder = '/home/william/repos/DataSets/tpchParquet/'

  # nation_tableName = "nation"
  # nation_columnNames = ["n_nationkey", "n_name", "n_regionkey", "n_comments"]
  # nation_filepath = dataFolder10 + "nation.tbl"
  # nation_columnTypes = ["int32", "str", "int32", "str"]  # pygdf/pandas style types
  # nation_gdf = cudf.read_csv(nation_filepath, delimiter='|', dtype=nation_columnTypes, names=nation_columnNames)

  # # print("nation_gdf")
  # # print(nation_gdf)
  
  # bc.create_table('nation_cudf', nation_gdf)
  # result1 = bc.sql("select n_nationkey, n_name, n_regionkey from main.nation_cudf where n_regionkey = 1", ['nation_cudf'])
  # now = result1.get()
  # print("blazing nation_cudf")
  # print(now)

  # bc.create_table('nation_csv', "/home/william/repos/DataSets/tpch10MbTest/nation_0_0.psv", delimiter='|', dtype=nation_columnTypes, names=nation_columnNames)
  # result4 = bc.sql("select n_nationkey, n_name, n_regionkey from main.nation_csv where n_regionkey = 1", ['nation_csv'])
  # now = result4.get()
  # # print("csv output")
  # print(now)
  # print("csv output done")

  # bc.create_table('nation_par', [parquetFolder + 'nation_0_0.parquet'])
  # result2 = bc.sql("select * from main.nation_par where n_nationkey < 20", ['nation_par'])
  # blaz_gdf = result2.get()
  # print("about to print gdf?")
  # print(blaz_gdf)
  # print("done print gdf?")
  # print("about to print gdf columns?")
  # print(blaz_gdf.columns)
  # print("done print gdf columns?")

  # bc.create_table('nation_bgdf', blaz_gdf)
  # result3 = bc.run_query("select * from main.nation_bgdf where n_nationkey < 10", ['nation_bgdf'])
  # rere = result3.get()
  # print(rere)

  # print("last")
  # print(now)
  # print("lasty")


  # gdf = cudf.read_csv('/home/william/repos/DataSets/Music.csv')
  # # gdf = cudf.read_csv('/home/william/repos/DataSets/tpch10Mb/nation.tbl')

  # bc.create_table('music',gdf)

  # result = bc.sql('SELECT * from main.music', ['music']).get()

  # print(result)
  
  
if __name__ == '__main__':
  main()
