import pyblazing.apiv2 as blazingsql
# from pyblazing.apiv2 import context
import cudf
from cudf.dataframe import DataFrame
import pandas as pd



def main():
  
  bc = blazingsql.make_context()  
  dataFolder10 = '/home/william/repos/DataSets/tpch10Mb/'
  dataFolder100 = '/home/william/repos/DataSets/DataSet100Mb/'
  
  parquetFolder = '/home/william/repos/DataSets/tpchParquet/'

  nation_tableName = "nation"
  nation_columnNames = ["n_nationkey", "n_name", "n_regionkey", "n_comments"]
  nation_filepath = dataFolder10 + "nation.tbl"
  nation_columnTypes = ["int32", "str", "int32", "str"]  # pygdf/pandas style types
  nation_gdf = cudf.read_csv(nation_filepath, delimiter='|', dtype=nation_columnTypes, names=nation_columnNames)

  print("nation_gdf")
  print(nation_gdf)
  
  bc.create_table('nation_cudf', nation_gdf)
  result1 = bc.sql("select n_nationkey, n_name, n_regionkey from main.nation_cudf where n_regionkey = 1", ['nation_cudf'])
  now = result1.get()
  print("blazing nation_cudf")
  print(now)

  bc.create_table('nation_csv', nation_filepath, delimiter='|', dtype=nation_columnTypes, names=nation_columnNames)
  result4 = bc.sql("select n_nationkey, n_name, n_regionkey from main.nation_csv where n_regionkey = 1", ['nation_csv'])
  now = result4.get()
  print("csv output")
  print(now)
  print("csv output done")

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

  print("last")
  print(now)
  print("lasty")
  
  
if __name__ == '__main__':
  main()
