import blazingdb.protocol
import blazingdb.protocol.interpreter
import blazingdb.protocol.orchestrator
import blazingdb.protocol.transport.channel
from connection import Connection

from ddlFunctions import ddlFunctions
from dmlFunctions import dmlFunctions
from dmlFunctions import inputData

from blazingdb.protocol.errors import Error
from blazingdb.messages.blazingdb.protocol.Status import Status
from blazingdb.protocol.interpreter import InterpreterMessage
from blazingdb.protocol.orchestrator import OrchestratorMessageType

from pygdf import read_csv

class PyBlazingTest:

    def main():
      cnn = Connection('/tmp/orchestrator.socket')
      result_token = ''

      try:
        print ('****************** Open Connection *********************')
        access_token = cnn.open()
      except Error as err:
        print(err)

      dml_client = dmlFunctions('/tmp/orchestrator.socket', '/tmp/ral.socket', '', access_token)

      filepath = "/home/william/repos/DataSets/TPCH50Mb/nation.psv"
      df = read_csv(filepath, delimiter='|', dtype=["int32","int64","int","int64"],names=["n_nationkey","n_name","n_regionkey","n_comments"])

      input_dataset = [inputData("nation",df)]
      result_token = dml_client.runQuery("select * from nation", input_dataset)
      


#
#       try:
#         print ('****************** Run Query ********************')
#         result_token = dml_client.runQuery('select * from Table')
#       except SyntaxError as err:
#         print(err)
#
#       try:
#         print ('****************** Get Result ********************')
#         resultResponse = dml_client.getResult(result_token)
#         print('GetResult Response')
#         print('  metadata:')
#         print('     status: %s' % resultResponse.metadata.status)
#         print('    message: %s' % resultResponse.metadata.message)
#         print('       time: %s' % resultResponse.metadata.time)
#         print('       rows: %s' % resultResponse.metadata.rows)
#         print('  fieldNames: %s' % list(resultResponse.fieldNames))
#         print('  values:')
#         print('    size: %s' % [value.size for value in resultResponse.values])
#       except SyntaxError as err:
#         print(err)

      print ('****************** Close Connection ********************')
      cnn.close()


    if __name__ == '__main__':
      main()
