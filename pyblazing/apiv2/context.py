from collections import OrderedDict
from enum import Enum

from urllib.parse import urlparse

from .bridge import internal_api

from .filesystem import FileSystem
from .sql import SQL
from .sql import ResultSet
from .datasource import build_datasource
import time


class BlazingContext(object):

    def __init__(self, connection = 'localhost:8889', dask_client = None):
        """
        :param connection: BlazingSQL cluster URL to connect to
            (e.g. 125.23.14.1:8889, blazingsql-gateway:7887).
        """

        # NOTE ("//"+) is a neat trick to handle ip:port cases
        parse_result = urlparse("//" + connection)
        orchestrator_host_ip = parse_result.hostname
        orchestrator_port = parse_result.port
        internal_api.SetupOrchestratorConnection(orchestrator_host_ip, orchestrator_port)

        # TODO percy handle errors (see above)
        self.connection = connection
        self.client = internal_api._get_client()
        self.fs = FileSystem()
        self.sqlObject = SQL()
        self.dask_client = dask_client;

    def __del__(self):
        # TODO percy clean next time
        # del self.sqlObject
        # del self.fs
        # del self.client
        pass

    def __repr__(self):
        return "BlazingContext('%s')" % (self.connection)

    def __str__(self):
        return self.connection

    # BEGIN FileSystem interface

    def localfs(self, prefix, **kwargs):
        return self.fs.localfs(self.client, prefix, **kwargs)

    def hdfs(self, prefix, **kwargs):
        return self.fs.hdfs(self.client, prefix, **kwargs)

    def s3(self, prefix, **kwargs):
        return self.fs.s3(self.client, prefix, **kwargs)

    def show_filesystems(self):
        print(self.fs)

    # END  FileSystem interface

    # BEGIN SQL interface

    # remove
    def create_table(self, table_name, input, **kwargs):
        ds = build_datasource(self.client, input, table_name, **kwargs)
        self.sqlObject.create_table(ds.table_name, ds)

        return ds

    def drop_table(self, table_name):
        return self.sqlObject.drop_table(table_name)

    # async
    def sql(self, sql, table_list = []):
        if (len(table_list) > 0):
            print("NOTE: You no longer need to send a table list to the .sql() funtion")
        return self.sqlObject.run_query(self.client, sql, self.dask_client)

    # END SQL interface


def make_context(connection = 'localhost:8889'):
    """
    :param connection: BlazingSQL cluster URL to connect to
           (e.g. 125.23.14.1:8889, blazingsql-gateway:7887).
    """
    bc = BlazingContext(connection)
    return bc
