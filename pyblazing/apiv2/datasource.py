from enum import Enum, unique

from urllib.parse import urlparse
from urllib.parse import ParseResult
from pathlib import PurePath

import pandas
import pyarrow

import cudf

from .bridge import internal_api


@unique
class Type(Enum):
    cudf = "RAPIDS CUDF DataFrame"
    pandas = "Pandas DataFrame"
    arrow = "PyArrow Table"
    csv = "CSV"
    parquet = "Apache Parquet"
    result_set = "BlazingSQL Non-distributed ResultSet"
    distributed_result_set = "BlazingSQL Distributed ResultSet"
    json = "JSON"
    orc = "Apache ORC"


# NOTE This class doesnt have any logic related to a remote call, is only for parsing the input
class Descriptor:

    def __init__(self, input):
        self._in_directory = False
        self._has_wildcard = False
        self._wildcard = ""

        self._uri = ""
        self._type = None
        self._files = []

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

        last_valid_type = None
        for file in list_input:
            if type(file) != str:
                raise Exception("If input into create_table is a list, it is expecting a list of path strings")

            url = self._to_url(file)
            path = self._to_path(url)
            self._type = self._type_from_path(path)

            if self._type == None:
                error_msg = "If input into create_table is a list, it is expecting a list of valid %s files. Invalid file: %s"
                error_msg = error_msg % (last_valid_type.value, file)
                raise Exception(error_msg)

            last_valid_type = self._type

        self._uri = str(path.parent)
        self._files = list_input


class DataSource:

    def __init__(self, client, descriptor, **kwargs):
        self._client = client
        self._descriptor = descriptor
        self._table_name = kwargs.get('table_name', None)

        # init the data source
        self._valid = self._load(**kwargs)

    def is_valid(self):
        return self._valid

    def descriptor(self):
        return self._descriptor

    def table_name(self):
        return self._table_name

    def _load(self, **kwargs):
        type = self._descriptor.type()

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
            csv_args = kwargs.get('csv_args', None)
            return self._load_csv(csv_args)
        elif type == Type.parquet:
            path = kwargs.get('path', None)
            return self._load_parquet()
        elif type == Type.json:
            json_lines = kwargs.get('json_lines', True)
            return self._load_json(json_lines)
        elif type == Type.orc:
            orc_args = kwargs.get('orc_args', None)
            return self._load_orc(orc_args)
        else:
            # TODO percy manage errors
            raise Exception("invalid datasource type")

        # TODO percy compare against bz proto Status_Success or Status_Error
        # here we need to use low level api pyblazing.create_table
        return False

    def _create_table_status_to_bool(self, return_result):
        if internal_api.Status.Error == return_result:
            return False
        elif internal_api.Status.Success == return_result:
            return True
        else:
            # TODO percy raise error
            pass

        return False

    def _load_cudf_df(self, cudf_df):
        self.cudf_df = cudf_df

        column_names = list(cudf_df.dtypes.index)
        column_dtypes = [internal_api.get_np_dtype_to_gdf_dtype(x) for x in cudf_df.dtypes]
        return_result = internal_api.create_table(
            self._client,
            self._table_name,
            type = internal_api.FileSchemaType.GDF,
            names = column_names,
            dtypes = column_dtypes,
            gdf = cudf_df
        )

        return self._create_table_status_to_bool(return_result)

    def _load_pandas_df(self, pandas_df):
        cudf_df = cudf.DataFrame.from_pandas(pandas_df)

        return self._load_cudf_df(self._table_name, cudf_df)

    def _load_arrow_table(self, arrow_table):
        pandas_df = arrow_table.to_pandas()

        return self._load_pandas_df(self._table_name, pandas_df)

    def _load_result_set(self, result_set):
        cudf_df = result_set.columns

        return self._load_cudf_df(self._table_name, cudf_df)

    def _load_distributed_result_set(self, distributed_result_set):
        print(distributed_result_set)
        return_result = internal_api.create_table(
            self.client,
            self._table_name,
            type = internal_api.FileSchemaType.DISTRIBUTED,
            resultToken = distributed_result_set[0].resultToken
        )

        return self._create_table_status_to_bool(return_result)

    def _load_csv(self, csv_args):
        # TODO percy manage datasource load errors
        if self._descriptor.files() == None:
            return False

        return_result = internal_api.create_table(
            self.client,
            self._table_name,
            type = internal_api.FileSchemaType.CSV,
            path = self._descriptor.files(),
            csv_args = csv_args
        )

        return self._create_table_status_to_bool(return_result)

    def _load_parquet(self):
        # TODO percy manage datasource load errors
        if self._descriptor.files() == None:
            return False

        return_result = internal_api.create_table(
            self._client,
            self._table_name,
            type = internal_api.FileSchemaType.PARQUET,
            path = self._descriptor.files()
        )

        return self._create_table_status_to_bool(return_result)

    def _load_json(self, lines):
        # TODO percy manage datasource load errors
        if self._descriptor.files() == None:
            return False

        return_result = internal_api.create_table(
            self._client,
            self._table_name,
            type = internal_api.FileSchemaType.JSON,
            path = self._descriptor.files(),
            lines = lines
        )

        return self._create_table_status_to_bool(return_result)

    def _load_orc(self, orc_args):
        # TODO percy manage datasource load errors
        if self._descriptor.files() == None:
            return False

        return_result = internal_api.create_table(
            self._client,
            self._table_name,
            type = internal_api.FileSchemaType.ORC,
            path = self._descriptor.files(),
            orc_args = orc_args
        )

        return self._create_table_status_to_bool(return_result)

# BEGIN DataSource builders


def from_cudf(cudf_df, table_name, descriptor):
    return DataSource(None, descriptor, table_name = table_name, cudf_df = cudf_df)


def from_pandas(pandas_df, table_name, descriptor):
    return DataSource(None, descriptor, table_name = table_name, pandas_df = pandas_df)


def from_arrow(arrow_table, table_name, descriptor):
    return DataSource(None, descriptor, table_name = table_name, arrow_table = arrow_table)


def from_result_set(result_set, table_name, descriptor):
    return DataSource(None, descriptor, table_name = table_name, result_set = result_set)


def from_distributed_result_set(result_set, table_name, descriptor):
    return DataSource(None, descriptor, table_name = table_name, result_set = result_set)


def from_csv(client, table_name, descriptor, csv_args):
    return DataSource(client, descriptor,
        table_name = table_name,
        csv_args = csv_args
    )


def from_parquet(client, table_name, descriptor):
    return DataSource(client, descriptor, table_name = table_name)


def from_json(client, table_name, descriptor, lines):
    return DataSource(client, descriptor, table_name = table_name, json_lines = lines)


def from_orc(client, table_name, descriptor, orc_args):
    return DataSource(client, descriptor, table_name = table_name, orc_args = orc_args)
# END DataSource builders

# BEGIN DataSource utils


def scan_datasource(client, directory, wildcard):
    files = internal_api.scan_datasource(client, directory, wildcard)
    return files


def build_datasource(client, input, table_name, **kwargs):
    ds = None

    descriptor = Descriptor(input)

    if descriptor.is_cudf():
        ds = from_cudf(input, table_name, descriptor)
    elif descriptor.is_pandas():
        ds = from_pandas(input, table_name, descriptor)
    elif descriptor.is_arrow():
        ds = from_arrow(input, table_name, descriptor)
    elif descriptor.is_result_set():
        ds = from_result_set(input, table_name, descriptor)
    elif descriptor.is_distributed_result_set():
        ds = from_distributed_result_set(input.metaToken, table_name, descriptor)
    elif descriptor.is_parquet():
        ds = from_parquet(client, table_name, descriptor)
    elif descriptor.is_csv():
        ds = from_csv(client, table_name, descriptor, **kwargs)

    # TODO percy raise error if ds is None

    return ds

# END DataSource utils
