import struct

import blazingdb.protocol

connection = blazingdb.protocol.UnixSocketConnection("/tmp/socket")
client = blazingdb.protocol.Client(connection)

name = 'Kharoly'
dni = '12345678'
age = 20

requestBuffer = struct.pack('ppi', name, dni, age)

responseBuffer = client.send(requestBuffer)

print(responseBuffer)