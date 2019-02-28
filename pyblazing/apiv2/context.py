from collections import OrderedDict
from enum import Enum

import pyblazing

from .filesystem import FileSystem
from .sql import SQL
from SocketServer import ForkingTCPServer


class BlazingContext(object):

    # connection (string) can be the unix socket path or the tcp host:port
    def __init__(self, connection):
        self.connection = connection
        self.client = pyblazing._get_client_internal(connection, 8890)
        self.fs = FileSystem()
        self.sql = SQL()

    def __repr__(self):
        return "BlazingContext('%s')" % (self.connection)

    def __str__(self):
        return self.connection

    # BEGIN FileSystem interface

    def hdfs(self, prefix, **kwargs):
        return self.fs.hdfs(self.client, prefix, **kwargs)

    def s3(self, prefix, **kwargs):
        return self.fs.s3(self.client, prefix, kwargs)

    def show_filesystems(self):
        print(self.fs)

    # END  FileSystem interface

    # BEGIN SQL interface

    def create_table(self, table_name, datasource):
        return self.sql.create_table(table_name, datasource)

    def drop_table(self, table_name):
        return self.sql.drop_table(table_name)

    # async
    def run_query(self, sql, table_names = []):
        return self.sql.run_query(self.client, sql, table_names)

    # END SQL interface

# fowrod/future
result = context.run_query("asdasdd", [...])
  - so we are hiding concepts like token

# TODO
#result.pass_owner()
#result.define_column_onersehe()

# TODO more doc for this case
gdf_content = result.get() ...start fechting content
  - manage the ipc 

Action Item:
- calcite catalog proper use
- define what specif objet u get from resul.get()
  - result.status (True/False)
  - result.colums/etc (cover non-distribution uses cases)
  - ...
- workflow using the conmsumttion of distrubion reseult sets
  - simulate rdd: previews (remote/local)
    - head
    - foot
  - result.get uses cases and implications
    - dask integration


gtc (I need to be align: expentations)
  - non-distribution (backend one api?)
    - demo: net flow demo (single node/single gpu) <--
      - multiple files ... 
    - getting starting with sql <--

  - distribution (new api?)
    - query many rals using the different gpus from dgx-2
    - dask stuff  




def make_context():
    # TODO percy we hardcode here becouse we know current ral has hardcoded this
    connection = '/tmp/orchestrator.socket'
    bc = BlazingContext(connection)
    return bc
