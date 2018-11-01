import pyarrow as pa
import pyblazing

arrow_table = pa.RecordBatchStreamReader('data/gpu.arrow').read_all()
df = arrow_table.to_pandas()
df = df[['swings', 'tractions']]
print(df)

tables = {'gpu_info': df}
sql = 'select swings+1, tractions+10 from main.gpu_info'
result_gdf = pyblazing.run_query_pandas(sql, tables)

print(sql)
print(result_gdf)
