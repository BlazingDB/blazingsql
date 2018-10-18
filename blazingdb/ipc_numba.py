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

from dmlFunctions import inputData
from pygdf import read_csv
from pygdf import _gdf
from pygdf import column
from pygdf import numerical
from pygdf import DataFrame
from libgdf_cffi import ffi
from numba import cuda


import pycuda.driver as drv
import pycuda.gpuarray as gpuarray
import numpy


from pygdf import _gdf


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
      ipd_handle = series._column._data.mem.get_ipc_handle()
      return ipd_handle
             


def read_sample_csv_file():
  filepath = "/home/william/repos/DataSets/TPCH50Mb/nation.psv"
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

  drv.init()
  dev = drv.Device(0)
  ctx_gpu = dev.make_context()

  hb = bytes(ipch._ipc_handle.handle)
  print("here is my handle source")
  print(len(hb)) 
  print(hb)
  res = client.send(hb)
  print(res)
  time.sleep(5)
  print("done wait")
  
  print(input_dataset[0]._gdfDataFrame)
  
  ctx_gpu.pop()


def server():
  print('waiting')

  connection = blazingdb.protocol.UnixSocketConnection(unix_path)
  server = blazingdb.protocol.Server(connection)

  def controller(h):
    print("here is my handle") 
    print(h)
      
    outcols = []
    print('receive handler')
    
    try:
        with cuda.open_ipc_array(h, shape=25, dtype=numpy.int32) as data_ptr :  
        
            print("here is the pointer")
            print(data_ptr)
            print(type(data_ptr)) 
            data = _gdf.unwrap_devary(data_ptr)
            print(type(data))
                    
            
    #         arr = numpy.arange(4)
    #         valid_ptr = cuda.to_device(arr)
    #         valid = _gdf.unwrap_devary(valid_ptr)
            valid = ffi.NULL
             
                    
            print('gpu array') 
            gdf_col = _gdf._columnview(25, data, valid, numpy.int32, 0)
            print('gdf')
            print(gdf_col)
            newcol = column.Column.from_cffi_view(gdf_col)
            outcols.append(newcol.view(numerical.NumericalColumn, dtype=newcol.dtype))
    
    except e:
        print("i got me an error")
        print(e)  
    
     # Build dataframe
    DF = DataFrame()
    DF["colA"] = outcols[0]
    print(DF)
    
#     drv.init()
#     dev = drv.Device(0)
#     ctx_gpu = dev.make_context()
#     x_ptr = drv.IPCMemoryHandle(bytearray(bytes(h)))
#     x_gpu = gpuarray.GPUArray((1, 10), numpy.int32, gpudata=x_ptr)
#     data = x_gpu
#     arr = numpy.arange(4)
#     valid_ptr = cuda.to_device(arr)
#     valid = _gdf.unwrap_devary(valid_ptr)
#     
#     print("types")
#     print(type(x_ptr))
#     print(type(x_gpu))
#     print(type(valid_ptr))
#     print(type(valid))
#            
#     print('gpu array') 
#     gdf_col = _gdf._columnview(25, data, valid, numpy.int32, 0)
#     print('gdf')
#     newcol = column.Column.from_cffi_view(gdf_col)
#     outcols.append(newcol.view(numerical.NumericalColumn, dtype=newcol.dtype))
    
    
    
#     ctx_gpu.pop()
    
      
#     print('receive handler')
#     print(h)
#     with cuda.open_ipc_array(h, shape=25, dtype=numpy.int32) as darr :
#       print('gpu array')
#       print(darr.copy_to_host())      
    return b'hi back!'

  server.handle(controller)


import time

if __name__ == '__main__':
  p1 = mp.Process(target=client)
  p2 = mp.Process(target=server)
  p2.start()
  time.sleep(0.5)
  p1.start()
