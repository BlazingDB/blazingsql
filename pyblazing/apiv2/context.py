from collections import OrderedDict
from enum import Enum

import pyblazing

from .filesystem import FileSystem
from .sql import SQL


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

    def run_query(self, sql, async_opts = 'TODO'):
        return self.sql.run_query(sql, async_opts)

    # END SQL interface


def make_context():
    # TODO percy we hardcode here becouse we know current ral has hardcoded this
    connection = '/tmp/orchestrator.socket'
    bc = BlazingContext(connection)
    return bc
