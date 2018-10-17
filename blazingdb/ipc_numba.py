# Sample source code from the Tutorial Introduction in the documentation.
import blazingdb.protocol
import blazingdb.protocol.interpreter
import blazingdb.protocol.orchestrator
import blazingdb.protocol.transport.channel
import multiprocessing as mp
import pycuda.gpuarray as gpuarray
import blazingdb.protocol
import blazingdb.protocol.interpreter
import blazingdb.protocol.orchestrator
import blazingdb.protocol.transport.channel

import blazingdb.protocol
import blazingdb.protocol.interpreter
import blazingdb.protocol.orchestrator
import blazingdb.protocol.transport.channel

from dmlFunctions import inputData
from pygdf import read_csv
from numba import cuda

import pycuda.driver as drv
import pycuda.gpuarray as gpuarray
import numpy

unix_path = '/tmp/demo.socket'

def _BuildDMLRequestSchema(query, tableGroupDto):
  tableGroupName = tableGroupDto['name']
  tables = []
  for index, t in enumerate(tableGroupDto['tables']):
    tableName = t['name']
    columnNames = t['columnNames']
    columns = []
    for i, c in enumerate(t['columns']):
      data = blazingdb.protocol.gdf.cudaIpcMemHandle_tSchema(reserved=c['data'])
      if c['valid'] is None:
        valid = blazingdb.protocol.gdf.cudaIpcMemHandle_tSchema(reserved=b'')
      else:
        valid = blazingdb.protocol.gdf.cudaIpcMemHandle_tSchema(reserved=c['valid'])

      dtype_info = blazingdb.protocol.gdf.gdf_dtype_extra_infoSchema(time_unit=c['dtype_info'])
      gdfColumn = blazingdb.protocol.gdf.gdf_columnSchema(data=data, valid=valid, size=c['size'],
                                                          dtype=c['dtype'], dtype_info=dtype_info,
                                                          null_count=c['null_count'])
      columns.append(gdfColumn)
    table = blazingdb.protocol.orchestrator.BlazingTableSchema(name=tableName, columns=columns,
                                                               columnNames=columnNames)
    tables.append(table)
  tableGroup = blazingdb.protocol.orchestrator.TableGroupSchema(tables=tables, name=tableGroupName)
  return blazingdb.protocol.orchestrator.DMLRequestSchema(query=query, tableGroup=tableGroup)

def get_one_ipc_df(input_dataset):
  ipd_handle = None
  tableGroup = {}
  tableGroup["name"] = ""
  tableGroup["tables"] = []
  for inputData in input_dataset:
    table = {}
    table["name"] = inputData._table_Name
    table["columns"] = []
    table["columnNames"] = []
    for name, series in inputData._gdfDataFrame._cols.items():
      table["columnNames"].append(name)
      cffiView = series._column.cffi_view
      print('dtype')
      print(cffiView.dtype)
      if series._column._mask is None:
        print('mask is none')
        ipd_handle = series._column._data.mem.get_ipc_handle()
        return ipd_handle


def read_sample_csv_file():
  filepath = "/home/aocsa/repos/DataSets/TPCH50Mb/nation.psv"
  df = read_csv(filepath, delimiter='|', dtype=["int32", "int64", "int", "int64"],
                names=["n_nationkey", "n_name", "n_regionkey", "n_comments"])
  time.sleep(1)
  print(df)
  input_dataset = [inputData("nation", df)]
  return input_dataset

def client():

  connection = blazingdb.protocol.UnixSocketConnection(unix_path)
  client = blazingdb.protocol.Client(connection)

  input_dataset = read_sample_csv_file()
  ipch = get_one_ipc_df(input_dataset)
  print(ipch)

  drv.init()
  dev = drv.Device(0)
  ctx_gpu = dev.make_context()

  res = client.send(bytes(ipch._ipc_handle.handle))
  print(res)
  ctx_gpu.pop()

def server():
  print('waiting')

  connection = blazingdb.protocol.UnixSocketConnection(unix_path)
  server = blazingdb.protocol.Server(connection)

  def controller(h):
    drv.init()
    dev = drv.Device(0)
    ctx_gpu = dev.make_context()

    print('receive handler')
    print(h)
    x_ptr = drv.IPCMemoryHandle(bytearray(bytes(h)))
    x_gpu = gpuarray.GPUArray((1, 10), numpy.int32, gpudata=x_ptr)
    print('gpu:  ', x_gpu.get())

    ctx_gpu.pop()
    return b'hi back!'

  server.handle(controller)


import time

if __name__ == '__main__':
  p1 = mp.Process(target=client)
  # p2 = mp.Process(target=server)
  # p2.start()
  time.sleep(0.5)
  p1.start()
