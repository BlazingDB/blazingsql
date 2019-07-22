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
from .sql import ResultSet
from .datasource import from_cudf
from .datasource import from_pandas
from .datasource import from_arrow
from .datasource import from_csv
from .datasource import from_parquet
from .datasource import from_result_set
from .datasource import from_distributed_result_set
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

    #remove
    def create_table(self, table_name, input, **kwargs):
        datasource = None

        if type(input) == cudf.DataFrame:
            datasource = from_cudf(input, table_name)
        elif type(input) == pandas.DataFrame:
            datasource = from_pandas(input, table_name)
        elif type(input) == pyarrow.Table:
            datasource = from_arrow(input, table_name)
        elif type(input) == internal_api.ResultSetHandle:
            datasource = from_result_set(input, table_name)
        elif hasattr(input, 'metaToken'):
            datasource = from_distributed_result_set(input.metaToken,table_name)
        elif type(input) == str or type(input) == list:

            if type(input) == str:
                uri = urlparse(input)
                path = PurePath(uri.path)
                paths = [input]
            else: # its a list
                if len(input) == 0:
                    raise Exception("Input into create_table was an empty list")
                elif type(input[0]) != str:
                    raise Exception("If input into create_table is a list, it is expecting a list of path strings")
                else:
                    uri = urlparse(input[0])
                    path = PurePath(uri.path)
                    paths = input

            if path.suffix == '.parquet':
                datasource = from_parquet(self.client, table_name, paths)
            elif path.suffix == '.csv' or path.suffix == '.psv' or path.suffix == '.tbl':
                # TODO percy duplicated code bud itnernal api desing remove this later
                csv_column_names = kwargs.get('names', [])
                csv_column_types = kwargs.get('dtype', [])
                csv_delimiter = kwargs.get('delimiter', '|')
                csv_skip_rows = kwargs.get('skiprows', 0)

                datasource = from_csv(self.client, table_name, paths,
                    csv_column_names,
                    csv_column_types,
                    csv_delimiter,
                    csv_skip_rows)

        else :
            raise Exception("Unknown data type " + str(type(input)) + " when creating table")

            # TODO percy dir

        self.sqlObject.create_table(table_name, datasource)

        # TODO percy raise exption here or manage the error

        return None

    def drop_table(self, table_name):
        return self.sqlObject.drop_table(table_name)

    # async
    def sql(self, sql, table_list=[]):
        if (len(table_list) > 0):
            print("NOTE: You no longer need to send a table list to the .sql() funtion")
        return self.sqlObject.run_query(self.client, sql,self.dask_client)

    # END SQL interface


def make_context(connection = 'localhost:8889'):
    """
    :param connection: BlazingSQL cluster URL to connect to
           (e.g. 125.23.14.1:8889, blazingsql-gateway:7887).
    """
    bc = BlazingContext(connection)
    return bc
