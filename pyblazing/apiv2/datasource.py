from enum import IntEnum, unique

from urllib.parse import urlparse
from urllib.parse import ParseResult
from pathlib import PurePath

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
        self._in_directory = False
        self._has_wildcard = False
        self._wildcard = ""

        self._uri = None
        self._type = None
        self._files = None

        if type(input) == cudf.DataFrame:
            self._type = Type.cudf
        elif type(input) == pandas.DataFrame:
            self._type = Type.pandas
        elif type(input) == pyarrow.Table:
            self._type = Type.arrow
        elif type(input) == internal_api.ResultSetHandle:
            self._type = Type.result_set
        elif hasattr(input, 'metaToken'):
            self._type = Type.distributed_result_set
        elif type(input) == list:
            self._parse_list(input)
        elif type(input) == str:
            if self._is_dir(input):
                list_input = scan_datasource(None, input, "")
                self._parse_list(list_input)
                self._in_directory = True
            elif self._is_wildcard(input):
                url = self._to_url(input)
                path = self._to_path(url)
                parent_dir = self._get_parent_dir(path)
                wildcard = self._get_wildcard(path)

                directory = ParseResult(
                    scheme = url.scheme,
                    netloc = url.netloc,
                    path = str(parent_dir),
                    params = url.params,
                    query = url.query,
                    fragment = url.fragment)

                directory = directory.geturl() + '/'

                list_input = scan_datasource(None, directory, wildcard)
                self._parse_list(list_input)
                self._has_wildcard = True
                self._wildcard = wildcard
            else:
                url = self._to_url(input)
                path = self._to_path(url)
                self._type = self._type_from_path(path)

                if self._type == None:
                    raise Exception("If input into create_table is a file path string, it is expecting a valid file extension")

                self._files = [input]

            self._uri = input

    def in_directory(self):
        return self._in_directory

    def has_wildcard(self):
        return self._has_wildcard

    def wildcard(self):
        return self._wildcard

    def uri(self):
        return self._uri

    def type(self):
        return self._type

    def files(self):
        return self._files

    # cudf.DataFrame in-gpu-memory
    def is_cudf(self):
        return self._type == Type.cudf

    # pandas.DataFrame in-memory
    def is_pandas(self):
        return self._type == Type.pandas

    # arrow file on filesystem or arrow in-memory
    def is_arrow(self):
        return self._type == Type.arrow

    # csv file on filesystem
    def is_csv(self):
        return self._type == Type.csv

    # parquet file on filesystem
    def is_parquet(self):
        return self._type == Type.parquet

    # blazing result set handle
    def is_result_set(self):
        return self._type == Type.result_set

    def is_distributed_result_set(self):
        return self._type == Type.distributed_result_set

    def _to_url(self, str_input):
        url = urlparse(str_input)
        return url

    def _to_path(self, url):
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

    def _get_wildcard(self, path):
        return path.name

    def _get_parent_dir(self, path):
        return path.parents[0]

    def _parse_list(self, list_input):
        if len(list_input) == 0:
            raise Exception("Input into create_table was an empty list")

        head = list_input[0]

        if type(head) != str:
            raise Exception("If input into create_table is a list, it is expecting a list of path strings")

        url = self._to_url(head)
        path = self._to_path(url)
        self._type = self._type_from_path(path)

        if self._type == None:
            raise Exception("If input into create_table is a list, it is expecting a list of valid file extension paths")

        self._uri = str(path.parent)
        self._files = list_input


class DataSource:

    def __init__(self, client, type, **kwargs):
        self.client = client

        self.table_name = None

        # TODO remove type and use Descriptor
        self.type = type

        # remove
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

    # def is_valid(self):
    #    return self.valid

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
        self.table_name = kwargs.get('table_name', None)

        if type == Type.cudf:
            cudf_df = kwargs.get('cudf_df', None)
            return self._load_cudf_df(cudf_df)
        elif type == Type.pandas:
            pandas_df = kwargs.get('pandas_df', None)
            return self._load_pandas_df(pandas_df)
        elif type == Type.arrow:
            arrow_table = kwargs.get('arrow_table', None)
            return self._load_arrow_table(arrow_table)
        elif type == Type.result_set:
            result_set = kwargs.get('result_set', None)
            return self._load_result_set(result_set)
        elif type == Type.distributed_result_set:
            result_set = kwargs.get('result_set', None)
            return self._load_distributed_result_set(result_set)
        elif type == Type.csv:
            files = kwargs.get('files', None)
            csv_column_names = kwargs.get('csv_column_names', [])
            csv_column_types = kwargs.get('csv_column_types', [])
            csv_delimiter = kwargs.get('csv_delimiter', '|')
            csv_skip_rows = kwargs.get('csv_skip_rows', 0)
            return self._load_csv(files,
                csv_column_names,
                csv_column_types,
                csv_delimiter,
                csv_skip_rows)
        elif type == Type.parquet:
            files = kwargs.get('files', None)
            return self._load_parquet(files)
        else:
            # TODO percy manage errors
            raise Exception("invalid datasource type")

        # TODO percy compare against bz proto Status_Success or Status_Error
        # here we need to use low level api pyblazing.create_table
        return False

    def _load_cudf_df(self, cudf_df):
        self.cudf_df = cudf_df

        column_names = list(cudf_df.dtypes.index)
        column_dtypes = [internal_api.get_np_dtype_to_gdf_dtype(x) for x in cudf_df.dtypes]
        return_result = internal_api.create_table(
            self.client,
            self.table_name,
            type = internal_api.FileSchemaType.GDF,
            names = column_names,
            dtypes = column_dtypes,
            gdf = cudf_df
        )

        # TODO percy see if we need to perform sanity check for cudf_df object
        self.valid = True

        return self.valid

    def _load_pandas_df(self, pandas_df):
        cudf_df = cudf.DataFrame.from_pandas(pandas_df)

        return self._load_cudf_df(self.table_name, cudf_df)

    def _load_arrow_table(self, arrow_table):
        pandas_df = arrow_table.to_pandas()

        return self._load_pandas_df(self.table_name, pandas_df)

    def _load_result_set(self, result_set):
        cudf_df = result_set.columns

        return self._load_cudf_df(self.table_name, cudf_df)

    def _load_distributed_result_set(self, distributed_result_set):
        print(distributed_result_set)
        internal_api.create_table(
            self.client,
            self.table_name,
            type = internal_api.FileSchemaType.DISTRIBUTED,
            resultToken = distributed_result_set[0].resultToken
        )

        self.valid = True

        return self.valid

    def _load_csv(self, files, column_names, column_types, delimiter, skip_rows):
        # TODO percy manage datasource load errors
        if files == None:
            return False

        self.files = files

        return_result = internal_api.create_table(
            self.client,
            self.table_name,
            type = internal_api.FileSchemaType.CSV,
            files = self.files,
            delimiter = delimiter,
            names = column_names,
            dtypes = internal_api.get_dtype_values(column_types),
            skip_rows = skip_rows
        )

        # TODO percy see if we need to perform sanity check for arrow_table object
        # return success or failed
        self.valid = return_result

        return self.valid

    def _load_parquet(self, files):
        # TODO percy manage datasource load errors
        if files == None:
            return False

        self.files = files

        return_result = internal_api.create_table(
            self.client,
            self.table_name,
            type = internal_api.FileSchemaType.PARQUET,
            files = self.files
        )

        # TODO percy see if we need to perform sanity check for arrow_table object
        # return success or failed
        self.valid = return_result

        return self.valid
# END remove

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


def from_csv(client, table_name, files, **kwargs):
    column_names = kwargs.get('names', [])
    column_types = kwargs.get('dtype', [])
    delimiter = kwargs.get('delimiter', '|')
    skip_rows = kwargs.get('skiprows', 0)

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


def scan_datasource(client, directory, wildcard):
    files = internal_api.scan_datasource(client, directory, wildcard)
    return files


def build_datasource(client, input, table_name, **kwargs):
    ds = None

    ds_descriptor = Descriptor(input)

    if ds_descriptor.is_cudf():
        ds = from_cudf(input, table_name)
    elif ds_descriptor.is_pandas():
        ds = from_pandas(input, table_name)
    elif ds_descriptor.is_arrow():
        ds = from_arrow(input, table_name)
    elif ds_descriptor.is_result_set():
        ds = from_result_set(input, table_name)
    elif ds_descriptor.is_distributed_result_set():
        ds = from_distributed_result_set(input.metaToken, table_name)
    elif ds_descriptor.is_parquet():
        ds = from_parquet(client, table_name, ds_descriptor.files())
    elif ds_descriptor.is_csv():
        ds = from_csv(client, table_name, ds_descriptor.files(), **kwargs)

    # TODO percy raise error if ds is None

    return ds

# END DataSource utils
