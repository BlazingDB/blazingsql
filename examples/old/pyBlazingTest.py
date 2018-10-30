import pygdf
import pyblazing


def main():
  
  nation_tableName = "nation"
#   nation_columnNames = ["n_nationkey", "n_name", "n_regionkey", "n_comments"]
  nation_columnNames = ["acol", "bcol", "ccol", "dcol"]
  nation_columnTypes = ["GDF_INT32", "GDF_INT64", "GDF_INT8", "GDF_INT64"]  # libgdf style types

  filepath = "data/nation.psv"
  nation_columnTypes = ["int32", "int64", "int", "int64"]  # pygdf/pandas style types
  gdf = pygdf.read_csv(filepath, delimiter='|', dtype=nation_columnTypes, names=nation_columnNames)
  print(gdf)

  tables = {nation_tableName: gdf}
  result = pyblazing.run_query('select acol + 1, ccol + 2 from main.nation', tables)
#   result = pyblazing.run_query('select n_nationkey, n_regionkey, n_name from main.nation', tables)
#   result = pyblazing.run_query('select n_nationkey, n_regionkey from main.nation where n_nationkey > 5', tables)
  print(result)

  
  
if __name__ == '__main__':
  main()
