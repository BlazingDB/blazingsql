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
from pygdf import utils

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
  result_token = dml_client.runQuery("select n_nationkey > 5 from main.nation", input_dataset)

  resultResponse = dml_client.getResult(result_token)

  print('  metadata:')
  print('     status: %s' % resultResponse.metadata.status)
  print('    message: %s' % resultResponse.metadata.message)
  print('       time: %s' % resultResponse.metadata.time)
  print('       rows: %s' % resultResponse.metadata.rows)

  def columnview_from_devary(devary, devary_valid, dtype=None):
    return _gdf._columnview(size=devary.size, data=_gdf.unwrap_devary(devary),
                       mask=_gdf.unwrap_devary(devary_valid), dtype=dtype or devary.dtype,
                       null_count=0)

  for i, c in enumerate(resultResponse.columns):
    with cuda.open_ipc_array(c.data, shape=c.size, dtype=_gdf.gdf_to_np_dtype(c.dtype)) as data_ptr:
      with cuda.open_ipc_array(c.valid, shape=utils.calc_chunk_size(c.size, utils.mask_bitsize), dtype=np.int8) as valid_ptr:
        gdf_col = columnview_from_devary(data_ptr, valid_ptr)
        outcols = []
        newcol = column.Column.from_cffi_view(gdf_col)

        # what is it? is  it required?:
        if newcol.dtype == np.dtype('datetime64[ms]'):
          outcols.append(newcol.view(DatetimeColumn, dtype='datetime64[ms]'))
        else:
          outcols.append(newcol.view(NumericalColumn, dtype=newcol.dtype))

        # Build dataframe
        df = DataFrame()
        for k, v in zip(resultResponse.columnNames, outcols):
          df['column' + str(k)] = v #todo chech concat
        print('  dataframe:')
        print(df)

  print('****************** Close Connection ********************')
  cnn.close()

if __name__ == '__main__':
  main()
