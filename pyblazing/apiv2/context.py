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

class CsvArgs():

    def __init__(self, path, **kwargs):
        self.path = path
        self.column_names = kwargs.get('names', [])
        self.column_types = kwargs.get('dtype', [])
        self.delimiter = kwargs.get('delimiter', None)  # the actual default value will be set in the validation funcion
        self.skiprows = kwargs.get('skiprows', 0)
        self.lineterminator = kwargs.get('lineterminator', '\n')
        self.skipinitialspace = kwargs.get('skipinitialspace', False)
        self.delim_whitespace = kwargs.get('delim_whitespace', False)
        self.header = kwargs.get('header', -1)
        self.nrows = kwargs.get('nrows', None)  # the actual default value will be set in the validation funcion
        self.skip_blank_lines = kwargs.get('skip_blank_lines', True)
        self.quotechar = kwargs.get('quotechar', '\"')
        self.quoting = kwargs.get('quoting', 0)
        self.doublequote = kwargs.get('doublequote', True)
        self.decimal = kwargs.get('decimal', '.')
        self.skipfooter = kwargs.get('skipfooter', 0)
        self.keep_default_na = kwargs.get('keep_default_na', True)
        self.na_filter = kwargs.get('na_filter', True)
        self.dayfirst = kwargs.get('dayfirst', False)
        self.thousands = kwargs.get('thousands', '\0')
        self.comment = kwargs.get('comment', '\0')
        self.true_values = kwargs.get('true_values', [])
        self.false_values = kwargs.get('false_values', [])
        self.na_values = kwargs.get('na_values', [])

    # Validate especific params when a csv or psv file is not sent
    def validate_empty(self):
        self.delimiter = ','
        self.nrows = -1

    # Validate input params
    def validation(self):

        # delimiter
        if self.delimiter == None:
            if self.path.suffix == '.csv':
                self.delimiter = ","
            elif self.path.suffix == '.psv': 
                self.delimiter = "|"
            else:
                self.delimiter = ","

        # lineterminator
        if isinstance(self.lineterminator, bool):
            raise TypeError("object of type 'bool' has no len()")
        elif isinstance(self.lineterminator, int):
            raise TypeError("object of type 'int' has no len()")
        if len(self.lineterminator) > 1:
            raise ValueError("Only length-1 decimal markers supported")

        # skiprows
        if self.skiprows == None or self.skiprows < 0:
            self.skiprows = 0
        elif isinstance(self.skiprows, str):
            raise TypeError("an integer is required")
        
        # header
        if self.header == -1 and len(self.column_names) == 0:
            self.header = 0
        if self.header == None or self.header < -1 :
            self.header = -1
        elif isinstance(self.header, str):
            raise TypeError("header must be integer or list of integers")

        # nrows
        if self.nrows == None:
            self.nrows = -1
        elif self.nrows < 0:
            raise ValueError("'nrows' must be an integer >= 0")

        # skipinitialspace
        if self.skipinitialspace  == None:
            raise TypeError("an integer is required")
        elif self.skipinitialspace  == False:
            self.skipinitialspace  = False
        else:
            self.skipinitialspace  = True

        # delim_whitespace
        if self.delim_whitespace == None or self.delim_whitespace == False:
            self.delim_whitespace = False
        elif isinstance(self.delim_whitespace, str):
            raise TypeError("an integer is required")
        else:
            self.delim_whitespace = True

        # skip_blank_lines
        if self.skip_blank_lines == None or isinstance(self.skip_blank_lines, str):
            raise TypeError("an integer is required")
        if self.skip_blank_lines != False:
            self.skip_blank_lines = True

        # quotechar
        if self.quotechar == None:
            raise TypeError("quotechar must be set if quoting enabled")
        elif isinstance(self.quotechar, int):
            raise TypeError("quotechar must be string, not int")
        elif isinstance(self.quotechar, bool):
            raise TypeError("quotechar must be string, not bool")
        elif len(self.quotechar) > 1 :
            raise TypeError("quotechar must be a 1-character string")

        # quoting
        if isinstance(self.quoting, int) :
            if self.quoting < 0 or self.quoting > 3 :
                raise TypeError("bad 'quoting' value")
        else:
            raise TypeError(" 'quoting' must be an integer")

        # doublequote
        if self.doublequote == None or not isinstance(self.doublequote, int):
            raise TypeError("an integer is required")
        elif self.doublequote != False:
            self.doublequote = True

        # decimal
        if self.decimal == None:
            raise TypeError("object of type 'NoneType' has no len()")
        elif isinstance(self.decimal, bool):
            raise TypeError("object of type 'bool' has no len()")
        elif isinstance(self.decimal, int):
            raise TypeError("object of type 'int' has no len()")
        if len(self.decimal) > 1:
            raise ValueError("Only length-1 decimal markers supported")

        # skipfooter
        if self.skipfooter == True or isinstance(self.skipfooter, str):
            raise TypeError("skipfooter must be an integer")
        elif self.skipfooter == False or self.skipfooter == None:
            self.skipfooter = 0
        if self.skipfooter < 0:
            self.skipfooter = 0

        # keep_default_na
        if self.keep_default_na  == False or self.keep_default_na  == 0:
            self.keep_default_na  = False
        else:
            self.keep_default_na  = True

        # na_filter
        if self.na_filter == False or self.na_filter == 0:
            self.na_filter = False
        else:
            self.na_filter = True

        # dayfirst
        if self.dayfirst == True or self.dayfirst == 1:
            self.dayfirst = True
        else:
            self.dayfirst = False

        # thousands
        if self.thousands == None:
            self.thousands = '\0'
        elif isinstance(self.thousands, bool):
            raise TypeError("object of type 'bool' has no len()")
        elif isinstance(self.thousands, int):
            raise TypeError("object of type 'int' has no len()")
        if len(self.thousands) > 1:
            raise ValueError("Only length-1 decimal markers supported")

        # comment
        if self.comment == None:
            self.comment = '\0'
        elif isinstance(self.comment, bool):
            raise TypeError("object of type 'bool' has no len()")
        elif isinstance(self.comment, int):
            raise TypeError("object of type 'int' has no len()")
        if len(self.comment) > 1:
            raise ValueError("Only length-1 decimal markers supported")

        # true_values
        if isinstance(self.true_values, bool):
            raise TypeError("'bool' object is not iterable")
        elif isinstance(self.true_values, int):
            raise TypeError("'int' object is not iterable")
        elif self.true_values == None:
            self.true_values = []
        elif isinstance(self.true_values, str):
            self.true_values = self.true_values.split(',')

        # false_values
        if isinstance(self.false_values, bool):
            raise TypeError("'bool' object is not iterable")
        elif isinstance(self.false_values, int):
            raise TypeError("'int' object is not iterable")
        elif self.false_values == None:
            self.false_values = []
        elif isinstance(self.false_values, str):
            self.false_values = self.false_values.split(',')

        # na_values
        if isinstance(self.na_values , int) or isinstance(self.na_values , bool):
            self.na_values  = str(self.na_values ).split(',')
        elif self.na_values  == None:
            self.na_values  = []
        

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
                csv_args = CsvArgs(path, **kwargs)
                csv_args.validation()
                datasource = from_csv(self.client, table_name, paths, csv_args)
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


def make_default_csv_arg(**kwargs):
    path = kwargs.get('path', '')
    csv_args = CsvArgs(path, **kwargs)
    csv_args.validate_empty()
    return csv_args
