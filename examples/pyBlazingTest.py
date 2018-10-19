from blazingdb.connector.connection import Connection
from blazingdb.connector.ddlFunctions import ddlFunctions
from blazingdb.connector.dmlFunctions import dmlFunctions
from blazingdb.connector.dmlFunctions import inputData
from blazingdb.protocol.errors import Error

from pygdf import read_csv
import time

import numpy as np
from numba import cuda
from pygdf import _gdf
from pygdf.buffer import Buffer
from pygdf.categorical import CategoricalColumn
from pygdf.datetime import DatetimeColumn
from pygdf.numerical import NumericalColumn
from pygdf import DataFrame
from pygdf import column


def main():
  cnn = Connection('/tmp/orchestrator.socket')

  try:
    print('****************** Open Connection *********************')
    cnn.open()
  except Error as err:
    print(err)

  ddl_client = ddlFunctions(cnn)
  nation_tableName = "nation"
  nation_columnNames = ["n_nationkey", "n_name", "n_regionkey", "n_comments"]
  nation_columnTypes = ["GDF_INT32", "GDF_INT64", "GDF_INT8", "GDF_INT64"]  # libgdf style types

  try:
    status = ddl_client.createTable(nation_tableName, nation_columnNames, nation_columnTypes)
  except Error as err:
    print("Error in creating table")
    print(err)

  dml_client = dmlFunctions(cnn)
  filepath = "/home/aocsa/repos/DataSets/TPCH50Mb/nation.psv"
  nation_columnTypes = ["int32", "int64", "int", "int64"]  # pygdf/pandas style types
  df = read_csv(filepath, delimiter='|', dtype=nation_columnTypes, names=nation_columnNames)

  time.sleep(1)

  print(df)

  input_dataset = [inputData(nation_tableName, df)]
  result_token = dml_client.runQuery("select * from nation where n_nationkey  < 5", input_dataset)

  resultResponse = dml_client.getResult(result_token)

  print('  metadata:')
  print('     status: %s' % resultResponse.metadata.status)
  print('    message: %s' % resultResponse.metadata.message)
  print('       time: %s' % resultResponse.metadata.time)
  print('       rows: %s' % resultResponse.metadata.rows)

  for i, c in enumerate(resultResponse.columns):
    with cuda.open_ipc_array(c.data, shape=c.size, dtype=_gdf.gdf_to_np_dtype(c.dtype)) as data_ptr:
      def from_cffi_view(cffi_view):
        data_mem, mask_mem = _gdf.cffi_view_to_column_mem(cffi_view)
        data_buf = Buffer(data_mem)
        mask = None
        return column.Column(data=data_buf, mask=mask)

      gdf_col = _gdf.columnview_from_devary(data_ptr)
      outcols = []
      newcol = from_cffi_view(gdf_col)
      if newcol.dtype == np.dtype('datetime64[ms]'):
        outcols.append(newcol.view(DatetimeColumn, dtype='datetime64[ms]'))
      else:
        outcols.append(newcol.view(NumericalColumn, dtype=newcol.dtype))

      # Build dataframe
      df = DataFrame()
      for k, v in zip(resultResponse.columnNames, outcols):
        df['column' + str(k)] = v

      print('  dataframe:')
      print(df)

  print('****************** Close Connection ********************')
  cnn.close()

if __name__ == '__main__':
  main()
