import numpy as np
import pandas as pd
import cudf as gd
import pyblazing


import time

def gen_data_frame(nelem, name, dtype):
    pdf = pd.DataFrame()
    pdf[name] = np.arange(nelem, dtype=dtype)
    pdf['data_2'] = np.array([0,0,0,0,1,1,1,1,2,2,2,2,3,3,3,3,4,4,4,4])
    df = gd.DataFrame.from_pandas(pdf)
    return df


gdf = gen_data_frame(20, 'swings', np.float32)

table = 'holas'
tables = {table: gdf}
t0 = time.time()
#keeps the handles in scope so we can ipc
gdfResult = pyblazing.run_query('select sum(main.swings), avg(main2.swings), sum(main2.swings), main.data_2 from main.holas as main inner join\
 main.holas as main2 on main2.swings = main.swings group by main.data_2', tables)
t1 = time.time()
print("time")
print(t1 - t0)
print("#RESULT_SET:")
print(gdfResult.columns)


#should call free handles andn result now! we can free in the ral yay without upgrading to cudf!!!

print("hi")
