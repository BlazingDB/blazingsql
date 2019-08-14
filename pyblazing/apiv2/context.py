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

                # Names and types
                csv_column_names = kwargs.get('names', [])
                csv_column_types = kwargs.get('dtype', [])

                # delimiter
                csv_delimiter = kwargs.get('delimiter')
                if csv_delimiter == None:
                    if path.suffix == '.csv':
                        csv_delimiter = ","
                    else:
                        csv_delimiter = "|"

                # lineterminator
                csv_lineterminator = kwargs.get('lineterminator', '\n')
                if csv_lineterminator == None:
                    csv_lineterminator = '\n'
                elif isinstance(csv_lineterminator, bool):
                    raise TypeError("object of type 'bool' has no len()")
                elif isinstance(csv_lineterminator, int):
                    raise TypeError("object of type 'int' has no len()")
                if len(csv_lineterminator) > 1:
                    raise ValueError("Only length-1 decimal markers supported")

                # skiprows
                csv_skiprows = kwargs.get('skiprows', 0)
                if csv_skiprows == None or csv_skiprows < 0:
                    csv_skiprows = 0

                # header
                csv_header = kwargs.get('header',-1)
                if csv_header == -1 and len(csv_column_names) == 0:
                    csv_header = 0
                if csv_header == None or csv_header < -1 :
                    csv_header = -1

                # nrows
                csv_nrows = kwargs.get('nrows')
                if csv_nrows == None:
                    csv_nrows = -1
                elif csv_nrows < 0:
                    raise ValueError("'nrows' must be an integer >= 0")

                # skipinitialspace
                csv_skipinitialspace = kwargs.get('skipinitialspace', False)
                if csv_skipinitialspace == None:
                   raise TypeError("an integer is required")
                elif csv_skipinitialspace == False:
                    csv_skipinitialspace = False
                else:
                    csv_skipinitialspace = True

                # delim_whitespace
                csv_delim_whitespace = kwargs.get('delim_whitespace', False)
                if csv_delim_whitespace == None or csv_delim_whitespace == False:
                    csv_delim_whitespace = False
                elif isinstance(csv_delim_whitespace, str):
                    raise TypeError("an integer is required")
                else:
                    csv_delim_whitespace = True

                # skip_blank_lines
                csv_skip_blank_lines = kwargs.get('skip_blank_lines', True)
                if csv_skip_blank_lines == None or isinstance(csv_skip_blank_lines, str):
                    raise TypeError("an integer is required")
                if csv_skip_blank_lines != False:
                    csv_skip_blank_lines = True

                # quotechar
                csv_quotechar = kwargs.get('quotechar', '\"')
                if csv_quotechar == None:
                    raise TypeError("quotechar must be set if quoting enabled")
                elif isinstance(csv_quotechar, int):
                    raise TypeError("quotechar must be string, not int")
                elif isinstance(csv_quotechar, bool):
                    raise TypeError("quotechar must be string, not bool")
                elif len(csv_quotechar) > 1 :
                    raise TypeError("quotechar must be a 1-character string")

                # quoting
                csv_quoting = kwargs.get('quoting', 0)
                if isinstance(csv_quoting, int) :
                    if csv_quoting < 0 or csv_quoting > 3 :
                        raise TypeError("bad 'quoting' value")
                else:
                    raise TypeError(" 'quoting' must be an integer")

                # doublequote
                csv_doublequote = kwargs.get('doublequote', True)
                if csv_doublequote == None or not isinstance(csv_doublequote, int):
                   raise TypeError("an integer is required")
                elif csv_doublequote != False:
                    csv_doublequote = True

                # decimal
                csv_decimal = kwargs.get('decimal', '.')
                if csv_decimal == None:
                    raise TypeError("object of type 'NoneType' has no len()")
                elif isinstance(csv_lineterminator, bool):
                    raise TypeError("object of type 'bool' has no len()")
                elif isinstance(csv_lineterminator, int):
                    raise TypeError("object of type 'int' has no len()")
                if len(csv_decimal) > 1:
                    raise ValueError("Only length-1 decimal markers supported")

                # skipfooter
                csv_skipfooter = kwargs.get('skipfooter', 0)
                if csv_skipfooter == True or isinstance(csv_skipfooter, str):
                    raise TypeError("skipfooter must be an integer")
                elif csv_skipfooter == False or csv_skipfooter == None:
                    csv_skipfooter = 0
                if csv_skipfooter < 0:
                    csv_skipfooter = 0

                # keep_default_na
                csv_keep_default_na = kwargs.get('keep_default_na', True)
                if csv_keep_default_na == False or csv_keep_default_na == 0:
                    csv_keep_default_na = False
                else:
                    csv_keep_default_na = True

                # na_filter
                csv_na_filter = kwargs.get('na_filter', True)
                if csv_na_filter == False or csv_na_filter == 0:
                    csv_na_filter = False
                else:
                    csv_na_filter = True

                # dayfirst
                csv_dayfirst = kwargs.get('dayfirst', False)
                if csv_dayfirst == True or csv_dayfirst == 1:
                    csv_dayfirst = True
                else:
                    csv_dayfirst = False

                # thousands
                csv_thousands = kwargs.get('thousands', '\0')
                if csv_thousands == None:
                    csv_thousands = '\0'
                elif isinstance(csv_thousands, bool):
                    raise TypeError("object of type 'bool' has no len()")
                elif isinstance(csv_thousands, int):
                    raise TypeError("object of type 'int' has no len()")
                if len(csv_thousands) > 1:
                    raise ValueError("Only length-1 decimal markers supported")

                datasource = from_csv(self.client, table_name, paths,
                    csv_column_names,
                    csv_column_types,
                    csv_delimiter,
                    csv_skiprows,
                    csv_lineterminator,
                    csv_header,
                    csv_nrows,
                    csv_skipinitialspace,
                    csv_delim_whitespace,
                    csv_skip_blank_lines,
                    csv_quotechar,
                    csv_quoting,
                    csv_doublequote,
                    csv_decimal,
                    csv_skipfooter,
                    csv_na_filter,
                    csv_keep_default_na,
                    csv_dayfirst,
                    csv_thousands)

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
