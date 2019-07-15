import pyarrow as pa
import pyblazing
from pyblazing import SchemaFrom
from pyblazing import DriverType, FileSystemType, EncryptionType
from collections import OrderedDict
from libgdf_cffi import ffi, libgdf

import time

class Chronometer:

  @staticmethod
  def makeUnstarted():
    return Chronometer()

  @staticmethod
  def makeStarted():
    return Chronometer.makeUnstarted().start()

  @staticmethod
  def show(chronometer, message):
    elapsed = chronometer.elapsed()
    Chronometer.items.append((message, elapsed))
    print('\033[32mTime: %s: %dms\033[0m' % (message, elapsed))

  items = []

  @staticmethod
  def show_resume():
    print('\033[32mResume')
    for item in Chronometer.items:
      print('%s: %dms' % item)
    print('\033[0m')

  def __init__(self):
    self.watch = Watch()
    self.startTime = 0
    self.elapsedTime = 0

  def start(self):
    self.startTime = self.watch.read()
    return self

  def stop(self):
    stopTime = self.watch.read()
    self.elapsedTime += stopTime - self.startTime
    return self

  def elapsed(self):
    return self.watch.read() - self.startTime + self.elapsedTime

  def reset(self):
    self.elapsedTime = 0
    return self


class Watch:

  def read(self):
    return int(time.time() * 1000)


def get_dtype_values(dtypes):
    values = []
    def gdf_type(type_name):
        dicc = {
            'str': libgdf.GDF_STRING,
            'date': libgdf.GDF_DATE64,
            'date64': libgdf.GDF_DATE64,
            'date32': libgdf.GDF_DATE32,
            'timestamp': libgdf.GDF_TIMESTAMP,
            'category': libgdf.GDF_CATEGORY,
            'float': libgdf.GDF_FLOAT32,
            'double': libgdf.GDF_FLOAT64,
            'float32': libgdf.GDF_FLOAT32,
            'float64': libgdf.GDF_FLOAT64,
            'short': libgdf.GDF_INT16,
            'long': libgdf.GDF_INT64,
            'int': libgdf.GDF_INT32,
            'int32': libgdf.GDF_INT32,
            'int64': libgdf.GDF_INT64,
        }
        if dicc.get(type_name):
            return dicc[type_name]
        return libgdf.GDF_INT64

    for key in dtypes:
        values.append( gdf_type(key))
    return values

def run_query(sql, sql_data, title):
    print(sql)
    chronometer = Chronometer.makeStarted()
    result_gdf = pyblazing.run_query_filesystem(sql, sql_data)
    print(result_gdf)
    Chronometer.show(chronometer, title)

import argparse
import helper as tools

def get_files(dir_path, filename, ngpus):
  response = []
  for i in range(ngpus):
    response.append(dir_path + '{}_{}_0.psv'.format(filename, i))
  return response

def main():
  parser = argparse.ArgumentParser(description='simple-distribution.')
  parser.add_argument("dir_path", help="folder full path, ex: ./tpch/data/")
  parser.add_argument("ngpus", help="Number of GPUs, ex: 2")

  print('*** Register a POSIX File System ***')
  args = parser.parse_args()
  dir_path = args.dir_path
  ngpus = args.ngpus

  fs_status = pyblazing.register_file_system(
      authority="tpch",
      type=FileSystemType.POSIX,
      root="/"
  )
  print(fs_status)

  column_names = ["c_custkey","c_name","c_address","c_nationkey","c_phone","c_acctbal","c_mktsegment","c_comment"]
  column_types = ["int32","int64","int64","int32","int64","float64","int64","int64"]
  customer_schema = pyblazing.register_table_schema(table_name='customer', type=SchemaFrom.CsvFile, path=dir_path + 'customer_0_0.psv', delimiter='|', dtypes=get_dtype_values(column_types), names=column_names)

  column_names = ['n_nationkey', 'n_name', 'n_regionkey', 'n_comments']
  column_types = ['int32', 'int64', 'int32', 'int64']
  nation_schema = pyblazing.register_table_schema(table_name='nation', type=SchemaFrom.CsvFile, path=dir_path + 'nation_0_0.psv', delimiter='|', dtypes=get_dtype_values(column_types), names=column_names)

  column_names = ['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment']
  column_types = ['int32', 'int32', 'int64', 'float64', 'int64', 'int64', 'int64', 'int64', 'int64']
  order_schema = pyblazing.register_table_schema(table_name='orders', type=SchemaFrom.CsvFile, path=dir_path + 'orders_0_0.psv', delimiter='|', dtypes=get_dtype_values(column_types), names=column_names)


  sql_data = {
      order_schema: get_files(dir_path, 'orders', ngpus),

      customer_schema: get_files(dir_path, 'customer', ngpus),
      
      nation_schema: get_files(dir_path, 'nation', ngpus)
  }

  # distributed sort
  queryId = 'TEST_01: distributed sort'
  sql = "select c_custkey, c_nationkey from main.customer order by c_nationkey"
  run_query(sql, sql_data, queryId)

  queryId = 'TEST_02: distributed sort'
  sql = "select c_custkey, c_acctbal from main.customer order by c_acctbal desc, c_custkey"
  run_query(sql, sql_data, queryId) 

  queryId = 'TEST_03: distributed sort'
  sql = "select o_orderkey, o_custkey from main.orders order by o_orderkey desc, o_custkey"
  run_query(sql, sql_data, queryId) 

  # distributed join
  queryId = 'TEST_01: distributed join'
  sql = "select count(c.c_custkey), sum(c.c_nationkey), n.n_regionkey from main.customer as c inner join main.nation as n on c.c_nationkey = n.n_nationkey group by n.n_regionkey"
  run_query(sql, sql_data, queryId)

  queryId = 'TEST_02: distributed join'
  sql = "select c.c_custkey, c.c_nationkey, n.n_regionkey from main.customer as c inner join main.nation as n on c.c_nationkey = n.n_nationkey where n.n_regionkey = 1 and c.c_custkey < 50"
  run_query(sql, sql_data, queryId)

  queryId = 'TEST_03: distributed join'
  sql = "select c.c_custkey, c.c_nationkey, n.o_orderkey from main.customer as c inner join main.orders as n on c.c_custkey = n.o_orderkey where n.o_orderkey < 1000"
  run_query(sql, sql_data, queryId)

  # distributed group_by
  queryId = 'TEST_01: distributed group_by'
  sql = "select count(c_custkey), sum(c_acctbal), sum(c_acctbal)/count(c_acctbal), min(c_custkey), max(c_nationkey), c_nationkey from main.customer group by c_nationkey"
  run_query(sql, sql_data, queryId)

  queryId = 'TEST_02: distributed group_by'
  sql = "select count(c_custkey), sum(c_acctbal), sum(c_acctbal)/count(c_acctbal), min(c_custkey), max(c_custkey), c_nationkey from main.customer where c_custkey < 50 group by c_nationkey"
  run_query(sql, sql_data, queryId)

  queryId = 'TEST_04: distributed group_by'
  sql = "select c_nationkey, count(c_acctbal) from main.customer group by c_nationkey, c_custkey"
  run_query(sql, sql_data, queryId)

  # all queries 

  queryId = 'TEST_01: all distributed operations'
  sql = "select c.c_custkey, c.c_nationkey, o.o_orderkey from main.customer as c inner join main.orders as o on c.c_custkey = o.o_orderkey inner join main.nation as n on c.c_nationkey = n.n_nationkey order by c_custkey"
  run_query(sql, sql_data, queryId)


if __name__ == '__main__':
    main()