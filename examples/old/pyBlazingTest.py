import cudf
from cudf.dataframe import DataFrame
import pyblazing
import pandas as pd


def main():
  
  dataFolder = '/home/william/repos/DataSets/tpch10Mb/'
  # dataFolder = '/home/william/repos/DataSets/DataSet100Mb/'
  
  parquetFolder = '/home/william/repos/DataSets/tpchParquet/'

  nation_tableName = "nation"
  nation_columnNames = ["n_nationkey", "n_name", "n_regionkey", "n_comments"]
#   nation_columnNames = ["acol", "bcol", "ccol", "dcol"]
  nation_columnTypes = ["GDF_INT32", "GDF_INT64", "GDF_INT8", "GDF_INT64"]  # libgdf style types
  nation_filepath = dataFolder + "nation.tbl"
  nation_columnTypes = ["int32", "int64", "int32", "int64"]  # pygdf/pandas style types
  # nation_gdf = cudf.read_csv(nation_filepath, delimiter='|', dtype=nation_columnTypes, names=nation_columnNames)
  # print(nation_gdf)
  
  region_tableName = "region"
  region_columnNames = ["r_regionkey", "r_name", "r_comments"]
  region_columnTypes = ["GDF_INT32", "GDF_INT64", "GDF_INT64"]  # libgdf style types
  region_filepath = dataFolder + "region.tbl"
  region_columnTypes = ["int32", "int64", "int64"]  # pygdf/pandas style types
  # region_gdf = cudf.read_csv(region_filepath, delimiter='|', dtype=region_columnTypes, names=region_columnNames)

  names_nation = ['n_nationkey', 'n_name', 'n_regionkey', 'n_comment']
  dtypes_nation = [3, 4, 3, 4]
    
  nation_table = pyblazing.register_table_schema(table_name='nation_csv', type=pyblazing.SchemaFrom.CsvFile, path=dataFolder + 'nation.tbl', delimiter='|', dtypes=dtypes_nation, names=names_nation)
  result_gdf = pyblazing.run_query_filesystem('select n_regionkey + 3 from main.nation_csv', {nation_table: [dataFolder + 'nation.tbl']})
  print(result_gdf.columns)

  # orders_tableName = "orders"
  # orders_columnNames = ["o_orderkey", "o_custkey", "o_orderstatus","o_totalprice", "o_orderdate", "o_orderpriority","o_clerk", "o_shippriority", "o_comment"]
  # # orders_columnTypes = ["GDF_INT64", "GDF_INT32", "GDF_INT64",  ]  # libgdf style types
  # orders_filepath = dataFolder + "orders.tbl"
  # orders_filepath = dataFolder + "orders_0_0.psv"
  
  # orders_columnTypes = ["int64", "int32", "int64", "float", "int32", "int64", "int64", "int64", "int64"]  # pygdf/pandas style types
  # orders_gdf = cudf.read_csv(orders_filepath, delimiter='|', dtype=orders_columnTypes, names=orders_columnNames)

  region_schema = pyblazing.register_table_schema(table_name='region', type=pyblazing.SchemaFrom.ParquetFile, path=parquetFolder + 'region_0_0.parquet')
  tables = {region_schema: [parquetFolder + 'region_0_0.parquet']}
  result_gdf = pyblazing.run_query_filesystem('select r_regionkey + 3 from main.region', tables)
  print(result_gdf.columns)

  lineitem_schema = pyblazing.register_table_schema(table_name='lineitem', type=pyblazing.SchemaFrom.ParquetFile, path= '/home/william/repos/DataSets/large_lineitem/lineitem_0_0.parquet')
  tables = {lineitem_schema: ['/home/william/repos/DataSets/large_lineitem/lineitem_0_0.parquet', '/home/william/repos/DataSets/large_lineitem/lineitem_0_0 (copy).parquet']}
  result_gdf = pyblazing.run_query_filesystem('select count(l_linenumber) from main.lineitem', tables)
  print(result_gdf.columns)

  # orders_schema = pyblazing.register_table_schema(table_name='orders', type=pyblazing.SchemaFrom.ParquetFile, path=parquetFolder + 'orders_0_0.parquet')
  # print(dir(orders_schema))
  # print(orders_schema.column_types)
  # tables = {orders_schema: [parquetFolder + 'orders_0_0.parquet']}
  # result_gdf = pyblazing.run_query_filesystem('select * from main.orders', tables)
  # print(result_gdf.columns)
  
  # table_pdf = pd.read_csv(region_filepath, delimiter = '|', usecols = [0], names = ['r_regionkey'])
  # region_gdf = DataFrame.from_pandas(table_pdf)
  # print(region_gdf)

  # part_tableName = "part"
  # part_columnNames = ["p_partkey", "p_name", "p_mfgr", "p_brand", "p_type", "p_size", "p_container", "p_retailprice", "p_comment"]
  # part_filepath = dataFolder + "part.tbl"
  # part_columnTypes = ["int32", "int64", "int64", "int64", "int64", "int32", "int64", "float", "int64"]  # pygdf/pandas style types
  # part_gdf = cudf.read_csv(part_filepath, delimiter='|', dtype=part_columnTypes, names=part_columnNames)

  # tables = {nation_tableName: nation_gdf, region_tableName: region_gdf}
  # # result = pyblazing.run_query('SELECT sum(n.n_nationkey), avg(n.n_nationkey), n.n_regionkey FROM main.nation AS n INNER JOIN main.region AS r ON n.n_regionkey = r.r_regionkey where n.n_regionkey < 3 group by n.n_regionkey', tables)
  # # result = pyblazing.run_query('SELECT n.n_nationkey, n.n_regionkey, (2*n.n_nationkey + n.n_regionkey) * (n.n_nationkey - r.r_regionkey) FROM main.nation AS n INNER JOIN main.region AS r ON n.n_regionkey = r.r_regionkey', tables)
  # result = pyblazing.run_query('select n_nationkey from main.nation where n_nationkey < 10 and n_nationkey >= 5', tables)
  # print(result.columns)

  # tables = {orders_tableName: orders_gdf}  
  # sql = "select count(o_orderkey), min(o_orderkey), max(o_orderkey) from main.orders where o_custkey >= 200 and o_orderkey < 300"
  # result = pyblazing.run_query(sql, tables)
  # print(result.columns) 

  # sql = "select count(o_custkey), min(o_custkey), max(o_custkey) from main.orders where o_orderkey >= 200 and o_custkey < 300"
  # result = pyblazing.run_query(sql, tables)
  # print(result.columns) 

  # sql = "select n1.n_regionkey, n1.n_nationkey, n2.n_nationkey from main.nation as n1 full outer join main.nation as n2 on n1.n_nationkey = n2.n_nationkey + 6 where n1.n_regionkey < 2"
  # tables = {nation_tableName: nation_gdf}  
  # result = pyblazing.run_query(sql, tables)
  # print(result.columns)

  # sql = "select n1.n_regionkey, sum(n1.n_nationkey) as s1, min(n1.n_nationkey) as m1, sum(n2.n_nationkey) as s2, min(n2.n_nationkey) as m2, count(*) as cstar from main.nation as n1 full outer join main.nation as n2 on n1.n_nationkey = n2.n_nationkey + 6 where n1.n_regionkey < 2 group by n1.n_regionkey "
  # # sql = "select n1.n_nationkey as n1key, n2.n_nationkey + 1000 as n2key, n1.n_nationkey + n2.n_nationkey from main.nation as n1 full outer join main.nation as n2 on n1.n_nationkey = n2.n_nationkey + 6"\
  # # sql = "select n1.n_nationkey + n2.n_nationkey from main.nation as n1 inner join main.nation as n2 on n1.n_nationkey = n2.n_nationkey"
  # # sql = "select n_nationkey + n_regionkey from main.nation "
  # tables = {nation_tableName: nation_gdf}  
  # result = pyblazing.run_query(sql, tables)
  # print(result.columns)

  # tables2 = {"mytable": result.columns}  
  # result2 = pyblazing.run_query("select * from main.mytable", tables)
  # print(result2.columns)

  # tables = {region_tableName: region_gdf}  
  # result = pyblazing.run_query('select r_regionkey  from main.region where r_regionkey < 3', tables)
  # print(result.columns)
  
#   tables = {part_tableName: part_gdf}
#   result = pyblazing.run_query('select count(p_partkey), sum(p_partkey), avg(p_partkey), max(p_partkey), min(p_partkey) from main.part where p_partkey < 100', tables)  
# #   result = pyblazing.run_query('select n_nationkey, n_regionkey from main.nation', tables)
# #   result = pyblazing.run_query('select acol, ccol, acol + ccol as summy from main.nation', tables)
# #   result = pyblazing.run_query('select acol, ccol from main.nation', tables)
#   print(result.columns)

  
  
#   tableName = "customer"
#   colNames = ["c_custkey","c_name","c_address","c_nationkey","c_number","c_acctbal","c_mktsegment","c_comment"]
#   colTypes = ["int32","int64","int64","int32","int64","float64","int64","int64"]
#   customer_gdf = pygdf.read_csv("/home/william/repos/DataSets/TPCH50Mb/customer_alt.psv",delimiter='|', dtype=colTypes, names=colNames)
#   print(customer_gdf)

  
  
if __name__ == '__main__':
  main()
