import blazingdb.protocol
import blazingdb.protocol.interpreter
import blazingdb.protocol.orchestrator
import blazingdb.protocol.transport.channel

from connection import Connection
from blazingdb.protocol.errors import Error
from blazingdb.messages.blazingdb.protocol.Status import Status
from blazingdb.protocol.interpreter import InterpreterMessage
from blazingdb.protocol.orchestrator import OrchestratorMessageType

class PyBlazingTest:
      
    def main():
      cnn = Connection('/tmp/orchestrator.socket', '/tmp/ral.socket')
      dml_client = dmlFunctions('/tmp/orchestrator.socket', '/tmp/ral.socket')
      ddl_client = ddlFunctions('/tmp/orchestrator.socket', '/tmp/ral.socket')
      result_token = ''
      
      try:
        print '****************** Open Connection *********************'
        cnn.open()
      except Error as err:
        print(err)
    
      try:
        print '****************** Run Query ********************'
        result_token = dml_client.runQuery('select * from Table')
      except SyntaxError as err:
        print(err)
        
      try:
        print '****************** Get Result ********************'
        resultResponse = dml_client.getResult(result_token)
        print('GetResult Response')
        print('  metadata:')
        print('     status: %s' % getResultResponse.metadata.status)
        print('    message: %s' % getResultResponse.metadata.message)
        print('       time: %s' % getResultResponse.metadata.time)
        print('       rows: %s' % getResultResponse.metadata.rows)
        print('  fieldNames: %s' % list(getResultResponse.fieldNames))
        print('  values:')
        print('    size: %s' % [value.size for value in getResultResponse.values])
      except SyntaxError as err:
        print(err)
    
      print '****************** Close Connection ********************'
      cnn.close()


    if __name__ == '__main__':
      main()
      
      