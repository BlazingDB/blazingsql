from collections import OrderedDict
from enum import Enum
from urllib.parse import urlparse
from pathlib import PurePath

import cudf
import pandas
import pyarrow

from .bridge import internal_api

from .filesystem import FileSystem
from .sql import SQL
from .datasource import from_cudf
from .datasource import from_pandas
from .datasource import from_arrow
from .datasource import from_csv
from .datasource import from_parquet
from .datasource import from_result_set
import time

class BlazingContext(object):

    # connection (string) can be the unix socket path or the tcp host:port
    def __init__(self, connection):
        self.connection = connection
        self.client = internal_api.__blazing__global_client
        self.fs = FileSystem()
        self.sql = SQL()

    def __del__(self):
        del self.sql
        del self.fs
        del self.client

        
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
        return self.fs.s3(self.client, prefix, kwargs)

    def show_filesystems(self):
        print(self.fs)

    # END  FileSystem interface

    # BEGIN SQL interface

    def create_table(self, table_name, input, **kwargs):
        datasource = None

        if type(input) == cudf.DataFrame:
            datasource = from_cudf(input)
        elif type(input) == pandas.DataFrame:
            datasource = from_pandas(input)
        elif type(input) == pyarrow.Table:
            datasource = from_arrow(input)
        elif type(input) == internal_api.ResultSetHandle:
            datasource = from_result_set(input)
        elif type(input) == str:
            uri = urlparse(input)
            path = PurePath(uri.path)

            if path.suffix == '.parquet':
                datasource = from_parquet(self.client, table_name, str(path))
            elif path.suffix == '.csv' or path.suffix == '.psv' or path.suffix == '.tbl':
                # TODO percy duplicated code bud itnernal api desing remove this later
                csv_column_names = kwargs.get('csv_column_names', [])
                csv_column_types = kwargs.get('csv_column_types', [])
                csv_delimiter = kwargs.get('csv_delimiter', '|')
                csv_skip_rows = kwargs.get('csv_skip_rows', 0)

                datasource = from_csv(self.client, table_name, str(path),
                    csv_column_names,
                    csv_column_types,
                    csv_delimiter,
                    csv_skip_rows)
        else :
            raise Exception("Unknown data type " + str(type(input)) + " when creating table")

            # TODO percy dir

        self.sql.create_table(table_name, datasource)

        # table = self.context.create_table(table_name, ds)

        # TODO percy raise exption here or manage the error

        return None

    def drop_table(self, table_name):
        return self.sql.drop_table(table_name)

    # async
    def run_query(self, sql, table_names = []):
        return self.sql.run_query(self.client, sql, table_names)

    # END SQL interface

# TODO percy remove these comments once the feedback is implemented
# # fowrod/future
# result = context.run_query("asdasdd", [...])
#   - so we are hiding concepts like token
#
# # TODO
# #result.pass_owner()
# #result.define_column_onersehe()
#
# # TODO more doc for this case
# gdf_content = result.get() ...start fechting content
#   - manage the ipc
#
# Action Item:
# - calcite catalog proper use
# - define what specif objet u get from resul.get()
#   - result.status (True/False)
#   - result.colums/etc (cover non-distribution uses cases)
#   - ...
# - workflow using the conmsumttion of distrubion reseult sets
#   - simulate rdd: previews (remote/local)
#     - head
#     - foot
#   - result.get uses cases and implications
#     - dask integration
#
#
# gtc (I need to be align: expentations)
#   - non-distribution (backend one api?)
#     - demo: net flow demo (single node/single gpu) <--
#       - multiple files ...
#     - getting starting with sql <--
#
#   - distribution (new api?)
#     - query many rals using the different gpus from dgx-2
#     - dask stuff


def make_context():
    # TODO percy we hardcode here becouse we know current ral has hardcoded this
    connection = '/tmp/orchestrator.socket'
    bc = BlazingContext(connection)
    return bc
