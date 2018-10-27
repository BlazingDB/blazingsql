import numpy as np
import pandas as pd
import pygdf as gd
import pyblazing


def gen_data_frame(nelem, name, dtype):
    pdf = pd.DataFrame()
    pdf[name] = np.arange(nelem, dtype=dtype)
    df = gd.DataFrame.from_pandas(pdf)
    return df


gdf = gen_data_frame(20, 'swings', np.float32)

table = 'holas'
tables = {table: gdf}
result = pyblazing.run_query('select swings from main.holas', tables)

print("#RESULT_SET:")

print('GetResult Response')
print('  metadata:')
print('     status: %s' % result.metadata.status)
print('    message: %s' % result.metadata.message)
print('       time: %s' % result.metadata.time)
print('       rows: %s' % result.metadata.rows)
print('  columnNames: %s' % list(result.columnNames))
print(type(result.columns))
print(result.columns)

print("hi")
