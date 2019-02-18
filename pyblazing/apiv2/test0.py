from components import make_context
from components import from_csv
from components import from_parquet
from components import from_dir

###  API usage ###

# blazingdb context
bc = make_context('/tmp/orchestrator.socket')
print(bc)

# 
# # file system
# bc.fs.hdfs('my_fs4', host = '127.0.0.1', port = 54310)
# bc.fs.s3('datastore', bucket_name = 'public_demo', access_key_id = 'ASDD', secret_key = 'SDFG')
# bc.fs.hdfs('prod_fs', host = '127.0.0.1', port = 54310)
# bc.fs.show()  # print all the file systems
# 
# # create table using blazing-io: from_x seems ok
# bc.sql.table('orders', from_csv('s3://my_fs4/home/dat.csv'))
# bc.sql.table('customer', from_parquet('/home/percy/part1.parquet'))
# bc.sql.table('lineitem', from_dir('hdfs://my_fs4/foo/tpch50mb-parquet/'))
# bc.sql.table('bigtable', from_dir('hdfs://prod_fs/percy/tpch3gb-csv/'))
# bc.sql.table('miserablesfb', from_dir('s3://prod_fs/graphistry/arrow-files/gpu/'))
# 
# 
# #from felipe ideas
# bc.sql.drop('miserablesfb') # forzar gpu free, blazingsql ownership ... if is last ref the delete
# 
# #from william
# #bc.sql.view('miserablesfb', sql, ["oder"])
# 
# #on-memory datasources: sin from_
# bc.sql.table('customers', from_gdf(gdf))
# bc.sql.table('customers', from_arrow(arrow))
# 
# # run query (async by default)
# result1 = bc.sql.run_query('select * from orders where foo', ["orders"])
# result2 = bc.sql.run_query('select * from lineitem, miserablesfb where blah', ["lineitem"])
# 
# # resultset operations (get() will call get result under the hood)
# colums1 = result1.get()
# colums2 = result2.get()

# interesting ideas on result objects
# result.collect.foreach(println) # where println is something like lambda x: print(x)
# result.show() => implicit call on .get

# TODO register mechanism for distributed tables (IN PROGRESS dask, distribution research)
