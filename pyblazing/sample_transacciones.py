import time
import pprint
from blazingsql import BlazingContext
from pyhive import hive
import os
import subprocess

# from dask.distributed import Client
# client = Client('127.0.0.1:8786')
# client.restart()
# bc = BlazingContext(dask_client=client, network_interface="lo")

nfiles = 4
os.environ['JAVA_HOME'] = os.getenv('CONDA_PREFIX')
os.environ['HADOOP_HOME'] = '/home/aocsa/apps/hadoop-2.8.4'

if (os.getenv('LD_LIBRARY_PATH') is None):
    os.environ['LD_LIBRARY_PATH'] = os.getenv('HADOOP_HOME') + "/lib/native/"
else:
    os.environ['LD_LIBRARY_PATH'] = os.getenv('LD_LIBRARY_PATH') + ":" + os.getenv('HADOOP_HOME') + "/lib/native/"

os.environ['PATH'] = os.getenv('PATH') + ":" + os.getenv('HADOOP_HOME') + "/bin"


callGlob = subprocess.run([os.getenv('HADOOP_HOME') + "/bin/hdfs", 'classpath', '--glob'], stdout=subprocess.PIPE)

os.environ['CLASSPATH'] = callGlob.stdout.decode('utf-8').rstrip()
os.environ['ARROW_LIBHDFS_DIR'] = os.getenv('PATH') + ":" + os.getenv('HADOOP_HOME') + "/lib/native/"

bc = BlazingContext()

authority = 'localhost:54310'
hdfs_host = 'localhost'
hdfs_port = 54310
hdfs_driver = 'libhdfs'
result, error_msg, fs = bc.hdfs(authority, host=hdfs_host, port=hdfs_port, user='aocsa', driver=hdfs_driver)

cursor = hive.connect('localhost').cursor() 

query = ' SELECT * FROM TRANSACCION_PARTICIONADA LIMIT 10'
cursor.execute(query)
print(query)
print(cursor.fetchall())

# cursor.execute('SHOW PARTITIONS TRANSACCION_PARTICIONADA')
# print('SHOW PARTITIONS TRANSACCION_PARTICIONADA')
# print(cursor.fetchall())

# cursor.execute('''DESCRIBE EXTENDED TRANSACCION_PARTICIONADA PARTITION(FECHA='2018-01-21')''')
# print(cursor.fetchall())

table = bc.create_table('TRANSACCION_PARTICIONADA', cursor, file_format='parquet')
# table = bc.create_table('TRANSACCION_PARTICIONADA', 'hdfs://localhost:54310/user/hive/warehouse/TRANSACCION_PARTICIONADA/part_id=2017/*', file_format='parquet')

print(table)
ddf = bc.sql(query)
print(query)
print(ddf)

