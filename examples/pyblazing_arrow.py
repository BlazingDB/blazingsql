import pyarrow as pa
import pyblazing

arrow_table = pa.RecordBatchStreamReader('data/gpu.arrow').read_all()

columns = ('swings', 'tractions')
filter_cols = [c.name for c in arrow_table.columns if not c.name in columns]
arrow_table = arrow_table.drop(filter_cols)

print(arrow_table)

tables = {'gpu_info': arrow_table}
sql = 'select * from main.gpu_info'
result_gdf = pyblazing.run_query_arrow(sql, tables)

print(sql)
print(result_gdf)
