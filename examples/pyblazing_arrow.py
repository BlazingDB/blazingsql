
# BlazingSQL with data from arrow table

arr = pa.RecordBatchStreamReader('data/gpu.arrow').read_all()
print(arr)
df = arr.to_pandas()
df = df[['swings', 'tractions']]
gdf = gd.DataFrame.from_pandas(df)
print(gdf)

table = 'tblArrow'
tables = {table: gdf}

result = pyblazing.run_query('select * from main.tblArrow', tables)

print('select * from main.tblArrow')
print("#RESULT_SET:")
print(result)

# BlazingSQL with data from pandas dataframe.


def gen_data_frame(nelem, name, dtype):
    pdf = pd.DataFrame()
    pdf[name] = np.arange(nelem, dtype=dtype)
    df = gd.DataFrame.from_pandas(pdf)
    return df


gdf = gen_data_frame(20, 'values', np.float32)

table = 'tblPandas'
tables = {table: gdf}
gdfResult = pyblazing.run_query('select * from main.tblPandas', tables)

print('select * from main.tblPandas')
print("#RESULT_SET:")
print(gdfResult)
