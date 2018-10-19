# Sample source code from the Tutorial Introduction in the documentation.
import blazingdb.protocol
import blazingdb.protocol.interpreter
import blazingdb.protocol.orchestrator
import blazingdb.protocol.transport.channel
import multiprocessing as mp
import blazingdb.protocol
import blazingdb.protocol.interpreter
import blazingdb.protocol.orchestrator
import blazingdb.protocol.transport.channel

import blazingdb.protocol
import blazingdb.protocol.interpreter
import blazingdb.protocol.orchestrator
import blazingdb.protocol.transport.channel

from blazingdb.connector.dmlFunctions import inputData
from pygdf import read_csv
from pygdf import _gdf
from pygdf import column
from pygdf import numerical
from pygdf import DataFrame
from libgdf_cffi import ffi
from numba import cuda
from pygdf.buffer import Buffer
import numpy


unix_path = '/tmp/demo.socket'

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
      ipd_handle = series._column._data.mem.get_ipc_handle()
      return ipd_handle
             


def read_sample_csv_file():
  filepath = "/home/aocsa/repos/DataSets/TPCH50Mb/nation.psv"
  df = read_csv(filepath, delimiter='|', dtype=["int32", "int64", "int", "int64"],
                names=["n_nationkey", "n_name", "n_regionkey", "n_comments"])
  
  
  time.sleep(1)
#   print(df)
  input_dataset = [inputData("nation", df)]
  return input_dataset

def client():

  connection = blazingdb.protocol.UnixSocketConnection(unix_path)
  client = blazingdb.protocol.Client(connection)

  input_dataset = read_sample_csv_file()
  ipch = get_one_ipc_df(input_dataset)
  print(ipch)
  
  print(input_dataset[0]._gdfDataFrame)

  hb = bytes(ipch._ipc_handle.handle)
  print("here is my handle source")
  print(len(hb)) 
  print(hb)
  res = client.send(hb)
  print(res)
  time.sleep(5)
  print("done wait")



def server():
  print('waiting')

  connection = blazingdb.protocol.UnixSocketConnection(unix_path)
  server = blazingdb.protocol.Server(connection)

  def controller(ipch):
    with cuda.open_ipc_array(ipch, shape=25, dtype=numpy.int32) as data_ptr:
      data = _gdf.unwrap_devary(data_ptr)
      print(type(data))

      print("here is the host pointer")
      print(type(data_ptr.copy_to_host()))
      print( data_ptr.copy_to_host() )

      # arr = numpy.arange(4)
      # valid_ptr = cuda.to_device(arr)
      # valid = _gdf.unwrap_devary(valid_ptr)

      print('gpu array')
      gdf_col = _gdf.columnview_from_devary(data_ptr)

      def from_cffi_view(cffi_view):
        data_mem, mask_mem = _gdf.cffi_view_to_column_mem(cffi_view)
        data_buf = Buffer(data_mem)
        mask = None
        return column.Column(data=data_buf, mask=mask)
      #column.Column.from_cffi_view
      outcols = []
      newcol = from_cffi_view(gdf_col)
      outcols.append(newcol.view(numerical.NumericalColumn, dtype=newcol.dtype))

      DF = DataFrame()
      DF["colA"] = outcols[0]
      print(DF)

    return b'hi back!'
  server.handle(controller)


import time

if __name__ == '__main__':
  p1 = mp.Process(target=client)
  p2 = mp.Process(target=server)
  p2.start()
  time.sleep(0.5)
  p1.start()
