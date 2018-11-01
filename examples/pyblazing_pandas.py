import pandas as pd
import pyblazing

column_names = ['n_nationkey', 'n_name', 'n_regionkey', 'n_comments']
column_types = {'n_nationkey': 'int32', 'n_regionkey': 'int64'}
nation_df = pd.read_csv("data/nation.psv", delimiter='|',
                        dtype=column_types, names=column_names)
nation_df = nation_df[['n_nationkey', 'n_regionkey']]

print(nation_df)

tables = {'nation': nation_df}

sql = 'select n_nationkey, n_regionkey, n_nationkey + n_regionkey as addition from main.nation'
result_gdf = pyblazing.run_query_pandas(sql, tables)

print(sql)
print(result_gdf)
