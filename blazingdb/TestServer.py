import struct

import blazingdb.protocol

connection = blazingdb.protocol.UnixSocketConnection("/tmp/socket")
server = blazingdb.protocol.Server(connection)

def controller(requestBuffer):
    
  payload = struct.unpack('7s8si', requestBuffer)
  print(payload)

  responseBuffer = b'Hola desde el server'

  return responseBuffer

  server.handle(controller)