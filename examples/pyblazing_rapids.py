import numpy as np
import pyblazing

def gen_data_frame(nelem, name, dtype):
    pdf = pd.DataFrame()
    pdf[name] = np.arange(nelem, dtype=dtype)
    df = DataFrame.from_pandas(pdf)
    return df

gdf = gen_data_frame(20, 'swings', np.float32)

table = 'holas'
tables = {table: gdf}
result = run_query('select swings from main.holas', tables)

print("hi")
