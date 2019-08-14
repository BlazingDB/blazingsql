from enum import IntEnum, unique

import pandas
import pyarrow

import cudf

from .bridge import internal_api

@unique
class Type(IntEnum):
    cudf = 0
    pandas = 1
    arrow = 2
    csv = 3
    parquet = 4
    result_set = 5
    distributed_result_set = 6

# NOTE This class doesnt have any logic related to a remote call, is only for parsing the input
class Descriptor:
    def __init__(self, input):
        self.uri = None
        self.type = None
        self.files = None

        if type(input) == cudf.DataFrame:
            self.type = Type.cudf
        elif type(input) == pandas.DataFrame:
            self.type = Type.pandas
        elif type(input) == pyarrow.Table:
            self.type = Type.arrow
        elif type(input) == internal_api.ResultSetHandle:
            self.type = Type.result_set
        elif hasattr(input, 'metaToken'):
            self.type = Type.distributed_result_set
        elif type(input) == list:
            self._parse_list(input)
        elif type(input) == str:
            if self._is_dir(input) or self._is_wildcard(input):
                list_input = scan_datasource(None, input)
                self._parse_list(list_input)
            else:
                path = self._to_path(input)
                self.type = self._type_from_path(path)
                
                if self.type == None:
                    raise Exception("If input into create_table is a file path string, it is expecting a valid file extension")
                
                self.files = [input]
            
            self.uri = input

    def uri(self):
        return self.uri

    def type(self):
        return self.type

    def files(self):
        return self.files

    def _to_path(self, str_input):
        url = urlparse(input)
        path = PurePath(url.path)
        return path

    def _type_from_path(self, path):
        if path.suffix == '.parquet':
            return Type.parquet
        
        if path.suffix == '.csv' or path.suffix == '.psv' or path.suffix == '.tbl':
            return Type.csv
        
        return None

    def _is_wildcard(self, str_input):
        return '*' in str_input

    def _is_dir(self, str_input):
        return str_input.endswith('/')

    def _parse_list(self, list_input):
        if len(list_input) == 0:
            raise Exception("Input into create_table was an empty list")
        
        head = list_input[0]
        
        if type(head) != str:
            raise Exception("If input into create_table is a list, it is expecting a list of path strings")
        
        path = self._to_path(head)
        self.type = self._type_from_path(path)
        
        if self.type == None:
            raise Exception("If input into create_table is a list, it is expecting a list of valid file extension paths")
        
        self.uri = str(path.parent)
        self.files = list_input


class DataSource:

    def __init__(self, client, type, **kwargs):
        self.client = client

        self.type = type

        #remove
        # declare all the possible fields (will be defined in _load)
        self.cudf_df = None  # cudf, pandas and arrow data will be passed/converted into this var
        self.csv = None
        self.parquet = None
        self.files = None

        # init the data source
        self.valid = self._load(type, **kwargs)

    def __repr__(self):
        return "TODO"

    def __str__(self):
        if self.valid == False:
            return 'Invalid datasource'

        type_str = {
            Type.cudf: 'cudf',
            Type.pandas: 'pandas',
            Type.arrow: 'arrow',
            Type.csv: 'csv',
            Type.parquet: 'parquet',
            Type.result_set: 'result_set'
        }

        return type_str[self.type] + ": " + self.files

    #def is_valid(self):
    #    return self.valid

    # cudf.DataFrame in-gpu-memory
    def is_cudf(self):
        return self.type == Type.cudf

    # pandas.DataFrame in-memory
    def is_pandas(self):
        return self.type == Type.pandas

    # arrow file on filesystem or arrow in-memory
    def is_arrow(self):
        return self.type == Type.arrow

    # csv file on filesystem
    def is_csv(self):
        return self.type == Type.csv

    # parquet file on filesystem
    def is_parquet(self):
        return self.type == Type.parquet

    # blazing result set handle
    def is_result_set(self):
        return self.type == Type.result_set

    def is_distributed_result_set(self):
        return self.type == Type.distributed_result_set

    # DEPRECATED NOTE percy deprecated
    def is_from_file(self):
        return self.type == Type.parquet or self.type == Type.csv

    def dataframe(self):
        # TODO percy add more support

        if self.type == Type.cudf:
            return self.cudf_df
        elif self.type == Type.pandas:
            return self.cudf_df
        elif self.type == Type.arrow:
            return self.cudf_df
        elif self.type == Type.result_set:
            return self.cudf_df

        return None

    def schema(self):
        if self.type == Type.csv:
            return self.csv
        elif self.type == Type.parquet:
            return self.parquet
        elif self.type == Type.cudf:
            return self.cudf_df

        return None

# BEGIN remove
    def _load(self, type, **kwargs):
        table_name = kwargs.get('table_name', None)
        if type == Type.cudf:
            cudf_df = kwargs.get('cudf_df', None)
            return self._load_cudf_df(table_name, cudf_df)
        elif type == Type.pandas:
            pandas_df = kwargs.get('pandas_df', None)
            return self._load_pandas_df(table_name, pandas_df)
        elif type == Type.arrow:
            arrow_table = kwargs.get('arrow_table', None)
            return self._load_arrow_table(table_name, arrow_table)
        elif type == Type.result_set:
            result_set = kwargs.get('result_set', None)
            return self._load_result_set(table_name, result_set)
        elif type == Type.distributed_result_set:
            result_set = kwargs.get('result_set', None)
            return self._load_distributed_result_set(table_name, result_set)
        elif type == Type.csv:
            files = kwargs.get('files', None)
            csv_column_names = kwargs.get('csv_column_names', [])
            csv_column_types = kwargs.get('csv_column_types', [])
            csv_delimiter = kwargs.get('csv_delimiter', '|')
            csv_skip_rows = kwargs.get('csv_skip_rows', 0)
            return self._load_csv(table_name, files,
                csv_column_names,
                csv_column_types,
                csv_delimiter,
                csv_skip_rows)
        elif type == Type.parquet:
            table_name = kwargs.get('table_name', None)
            files = kwargs.get('files', None)
            return self._load_parquet(table_name, files)
        else:
            # TODO percy manage errors
            raise Exception("invalid datasource type")

        # TODO percy compare against bz proto Status_Success or Status_Error
        # here we need to use low level api pyblazing.create_table
        return False

    def _load_cudf_df(self, table_name, cudf_df):
        self.cudf_df = cudf_df

        column_names = list(cudf_df.dtypes.index)
        column_dtypes = [internal_api.get_np_dtype_to_gdf_dtype(x) for x in cudf_df.dtypes]
        return_result = internal_api.create_table(
            self.client,
            table_name,
            type = internal_api.FileSchemaType.GDF,
            names = column_names,
            dtypes = column_dtypes,
            gdf = cudf_df
        )

        # TODO percy see if we need to perform sanity check for cudf_df object
        self.valid = True

        return self.valid

    def _load_pandas_df(self, table_name, pandas_df):
        cudf_df = cudf.DataFrame.from_pandas(pandas_df)

        return self._load_cudf_df(table_name, cudf_df)

    def _load_arrow_table(self, table_name, arrow_table):
        pandas_df = arrow_table.to_pandas()

        return self._load_pandas_df(table_name, pandas_df)

    def _load_result_set(self, table_name, result_set):
        cudf_df = result_set.columns

        return self._load_cudf_df(table_name, cudf_df)

    def _load_distributed_result_set(self, table_name, distributed_result_set):
        print(distributed_result_set)
        internal_api.create_table(
            self.client,
            table_name,
            type = internal_api.FileSchemaType.DISTRIBUTED,
            resultToken = distributed_result_set[0].resultToken
        )

        self.valid = True

        return self.valid


    def _load_csv(self, table_name, files, column_names, column_types, delimiter, skip_rows):
        # TODO percy manage datasource load errors
        if files == None:
            return False

        self.files = files

        return_result = internal_api.create_table(
            self.client,
            table_name,
            type = internal_api.FileSchemaType.CSV,
            files = self.files,
            delimiter = delimiter,
            names = column_names,
            dtypes = internal_api.get_dtype_values(column_types),
            skip_rows = skip_rows
        )

        # TODO percy see if we need to perform sanity check for arrow_table object
        #return success or failed
        self.valid = return_result

        return self.valid

    def _load_parquet(self, table_name, files):
        # TODO percy manage datasource load errors
        if files == None:
            return False

        self.files = files

        return_result = internal_api.create_table(
            self.client,
            table_name,
            type = internal_api.FileSchemaType.PARQUET,
            files = self.files
        )

        # TODO percy see if we need to perform sanity check for arrow_table object
        #return success or failed
        self.valid = return_result

        return self.valid
#END remove

# BEGIN DataSource builders


def from_cudf(cudf_df, table_name):
    return DataSource(None, Type.cudf, table_name = table_name, cudf_df = cudf_df)


def from_pandas(pandas_df, table_name):
    return DataSource(None, Type.pandas, table_name = table_name, pandas_df = pandas_df)


def from_arrow(arrow_table, table_name):
    return DataSource(None, Type.arrow, table_name = table_name, arrow_table = arrow_table)

def from_result_set(result_set, table_name):
    return DataSource(None, Type.result_set, table_name = table_name, result_set = result_set)

def from_distributed_result_set(result_set, table_name):
    return DataSource(None, Type.distributed_result_set, table_name = table_name, result_set = result_set)


def from_csv(client, table_name, files, column_names, column_types, delimiter, skip_rows):
    return DataSource(client, Type.csv,
        table_name = table_name,
        files = files,
        csv_column_names = column_names,
        csv_column_types = column_types,
        csv_delimiter = delimiter,
        csv_skip_rows = skip_rows
    )


def from_parquet(client, table_name, files):
    return DataSource(client, Type.parquet, table_name = table_name, files = files)

# END DataSource builders

# BEGIN DataSource utils

def scan_datasource(client, uri):
    files = internal_api.scan_datasource(client, uri)
    return files

# END DataSource utils
