from collections import OrderedDict
from enum import Enum

import pyblazing

from filesystem import FileSystem


class DataSource:

    def __init__(self):
        pass

    def is_valid(self):
        pass

    # cudf.DataFrame in-gpu-memory
    def is_cudf(self):
        pass

    # pandas.DataFrame in-memory
    def is_pandas(self):
        pass

    # arrow file on filesystem or arrow in-memory
    def is_arrow(self):
        pass

    # csv file on filesystem
    def is_csv(self):
        pass

    # parquet file on filesystem
    def is_parquet(self):
        pass

    # dir which contains any supported file on file system (csv, parquet or arrow)
    def is_dir(self):
        pass

# BEGIN DataSource builders


def from_cudf():
    pass


def from_pandas():
    pass


def from_arrow():
    pass


# path is file system transparent
def from_csv(path):
    pass


# path is file system transparent
def from_parquet(path):
    pass


# path is file system transparent
def from_dir(path, hint = 'auto can be parquet arrow csv'):
    pass

# END DataSource builders


# Maintains the resulset and the token after the run_query
class ResultSet:

    # this will call the get_result api
    def get(self):
        pass

    # TODO see Rodriugo proposal for interesting actions/operations here


class SQL(object):

    def __init__(self):
        pass

    # ds is the DataSource object
    def table(self, name, ds):
        pass

    # TODO percy this is to save materialized tables avoid reading from the data source
    def view(self, name, sql):
        pass

    # TODO percy drops should be here but this will be later (see Felipe proposal free)
    def drop(self, name):
        pass

    # TODO percy think about William proposal, launch, token split and distribution use case
    # return result obj ... by default is async
    def run_query(self, sql, async_opts = 'TODO'):
        rs = ResultSet()
        # TODO percy
        return rs


class BlazingContext(object):

    # connection (string) can be the unix socket path or the tcp host:port
    def __init__(self, connection):
        self.connection = connection
        self.client = pyblazing._get_client_internal(connection, 8890)
        self.fs = FileSystem()

    def __repr__(self):
        return "BlazingContext('%s')" % (self.connection)

    def __str__(self):
        return self.connection

    def hdfs(self, prefix, **kwargs):
        return self.fs.hdfs(self.client, prefix, **kwargs)

    def s3(self, prefix, **kwargs):
        return self.fs.s3(self.client, prefix, kwargs)

    def show_filesystems(self):
        self.fs.show()


def make_context():
    # TODO percy we hardcode here becouse we know current ral has hardcoded this
    connection = '/tmp/orchestrator.socket'
    bc = BlazingContext(connection)
    return bc
