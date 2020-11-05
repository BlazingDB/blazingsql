import time
import pprint
import cudf
from blazingsql import BlazingContext
from dask.distributed import Client
from pyhive import hive
import os
import subprocess 

# client = Client('127.0.0.1:8786')
# client.restart()
# bc = BlazingContext(dask_client=client, network_interface="lo")

bc = BlazingContext()

authority = 'localhost:54310'
hdfs_host = 'localhost'
hdfs_port = 54310
hdfs_driver = 'libhdfs'
result, error_msg, fs = bc.hdfs(authority, host=hdfs_host, port=hdfs_port, user='aocsa', driver=hdfs_driver)

cursor = hive.connect('localhost').cursor() 

table = bc.create_table('ptransactions', cursor, file_format='parquet')
for i in range(11):
    query = "SELECT * FROM ptransactions where t_year=2017 and t_company_id={t_company_id} LIMIT 10".format(t_company_id=i)
    ddf = bc.sql(query)
    print(query)

    if isinstance(ddf, cudf.DataFrame):
        print(ddf)
    else:
        print(ddf.compute())