import pygdf as cudf
import pyblazing

column_names = ['n_nationkey', 'n_name', 'n_regionkey', 'n_comments']
column_types = ['int32', 'int64', 'int32', 'int64']
nation_gdf = cudf.read_csv("data/nation.psv", delimiter='|',
                           dtype=column_types, names=column_names)

print(nation_gdf)

tables = {'nation': nation_gdf}

sql = 'select n_nationkey, n_regionkey, n_nationkey + n_regionkey as addition from main.nation'
result_gdf = pyblazing.run_query(sql, tables)

print(sql)
print(result_gdf)
