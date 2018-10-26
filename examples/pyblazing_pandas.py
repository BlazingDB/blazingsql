import pyarrow as pa


#arr = pa.RecordBatchStreamReader('/gpu.arrow').read_all()
#print(arr)
#df = arr.to_pandas()
#df = df[['swings', 'tractions']]
#gdf = gd.DataFrame.from_pandas(df)
#gdf._cols["swings"]._column.data.mem.get_ipc_handle()._ipc_handle.handle
# print(gdf.columns)
# print(gdf._cols["swings"]._column.dtype)
# gdf._cols["swings"]._column.cffi_view.size

gdf = gen_data_frame(20, 'swings', np.float32)


table = 'holas'
tables = {table: gdf}

#client = _get_client()
#_reset_table(client, table, gdf)

#result = run_query('select swings, tractions from main.holas', tables)
result = run_query('select swings from main.holas', tables)

print("hi")
