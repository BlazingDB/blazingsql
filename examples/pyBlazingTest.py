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
  filepath = "/home/william/repos/DataSets/TPCH50Mb/nation.psv"
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

  print('  dataframe:')
  print(resultResponse.dataFrame)

  print('****************** Closing Connection ********************')
  cnn.close()

  print('****************** Closed Connection ********************')
  
if __name__ == '__main__':
  main()
