import blazingdb.protocol
import blazingdb.protocol.orchestrator


def main():
  connection = blazingdb.protocol.UnixSocketConnection('/tmp/socket')
  server = blazingdb.protocol.Server(connection)

  def controller(requestBuffer):
    request = blazingdb.protocol.orchestrator.DMLRequestFrom(requestBuffer)

    print(request.header)
    print(request.payload.query)

    responseBuffer = \
      blazingdb.protocol.orchestrator.MakeDMLResponse('t-o-k-e-n')

    return responseBuffer

  server.handle(controller)


if __name__ == '__main__':
  main()