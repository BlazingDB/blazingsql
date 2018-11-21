import cudf
import pyblazing


def main():
  
  nation_tableName = "nation"
  nation_columnNames = ["n_nationkey", "n_name", "n_regionkey", "n_comments"]
#   nation_columnNames = ["acol", "bcol", "ccol", "dcol"]
  nation_columnTypes = ["GDF_INT32", "GDF_INT64", "GDF_INT8", "GDF_INT64"]  # libgdf style types
  nation_filepath = "data/nation.psv"
  nation_columnTypes = ["int32", "int64", "int", "int64"]  # cudf/pandas style types
  nation_gdf = cudf.read_csv(nation_filepath, delimiter='|', dtype=nation_columnTypes, names=nation_columnNames)
  print(nation_gdf)
  
  region_tableName = "region"
  region_columnNames = ["r_regionkey", "r_name", "r_comments"]
  region_columnTypes = ["GDF_INT8", "GDF_INT64", "GDF_INT64"]  # libgdf style types
  region_filepath = "data/region.psv"
  region_columnTypes = ["int", "int64", "int64"]  # cudf/pandas style types
  region_gdf = cudf.read_csv(region_filepath, delimiter='|', dtype=region_columnTypes, names=region_columnNames)
  print(region_gdf)

  tables = {nation_tableName: nation_gdf, region_tableName: region_gdf}
  result = pyblazing.run_query('SELECT n.n_nationkey, n.n_regionkey, r.r_regionkey FROM main.nation AS n INNER JOIN main.region AS r ON n.n_regionkey = r.r_regionkey', tables)
  
#   tables = {region_tableName: region_gdf}  
#   result = pyblazing.run_query('select r_regionkey from main.region', tables)
  
#   tables = {nation_tableName: nation_gdf}  
#   result = pyblazing.run_query('select n_nationkey + 1, n_regionkey + 2 from main.nation', tables)
#   result = pyblazing.run_query('select acol, ccol, acol + ccol as summy from main.nation', tables)
#   result = pyblazing.run_query('select acol, ccol from main.nation', tables)
  print(result.columns)

  
  
if __name__ == '__main__':
  main()
