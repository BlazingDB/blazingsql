from enum import Enum, unique

from urllib.parse import urlparse
from urllib.parse import ParseResult
from pathlib import PurePath

import pandas
import pyarrow

import cudf
import dask_cudf

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
    dask_cudf = "Dask CUDF"


# NOTE This is only for parsing the input, will call the scan_datasource service if necessary
class Descriptor:

    def __init__(self, input, file_format):
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
            self._parse_list(input, file_format)
        elif type(input) == str:
            if self._is_dir(input):
                list_input = scan_datasource(None, input, "")
                self._parse_list(list_input, file_format)
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
                self._parse_list(list_input, file_format)
                self._has_wildcard = True
                self._wildcard = wildcard
            else:
                url = self._to_url(input)
                path = self._to_path(url)
                self._type = self._type_from_path(path, file_format)

                if self._type == None:
                    raise Exception("If input into create_table is a file path string, it is expecting a valid file extension. Alternatively you can set the optional parameter 'file_format' to one of the supported types: 'parquet','orc','csv','json' ")

                self._files = [input]

            self._uri = input
        elif type(input) == dask_cudf.core.DataFrame:
            self._type = Type.dask_cudf

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

    # parquet file on filesystem
    def is_json(self):
        return self._type == Type.json

    # parquet file on filesystem
    def is_orc(self):
        return self._type == Type.orc

    # dask_cudf in gpu-memory distributed_result_set
    def is_dask_cudf(self):
        return self._type == Type.dask_cudf

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

    def _type_from_path(self, path, file_format):

        if file_format is not None:
            if not any([type == file_format for type in ['parquet','orc','csv','json']]):
                print("WARNING: file_format does not match any of the supported types: 'parquet','orc','csv','json'")

        if file_format == 'parquet' or path.suffix == '.parquet':
            return Type.parquet

        if file_format == 'csv' or path.suffix == '.csv' or path.suffix == '.psv' or path.suffix == '.tbl':
            return Type.csv

        if file_format == 'json' or path.suffix == '.json':
            return Type.json

        if file_format == 'orc' or path.suffix == '.orc':
            return Type.orc

        return None

    def _is_wildcard(self, str_input):
        return '*' in str_input

    def _is_dir(self, str_input):
        return str_input.endswith('/')

    def _get_wildcard(self, path):
        return path.name

    def _get_parent_dir(self, path):
        return path.parents[0]

    def _parse_list(self, list_input, file_format):
        if len(list_input) == 0:
            raise Exception("Input into create_table was an empty list")

        last_valid_type = None
        for file in list_input:
            if type(file) != str:
                raise Exception("If input into create_table is a list, it is expecting a list of path strings")

            url = self._to_url(file)
            path = self._to_path(url)
            self._type = self._type_from_path(path, file_format)

            if self._type == None:
                error_msg = "If input into create_table is a list of file path string, it is expecting the strings to have a valid file extension. Alternatively you can set the optional parameter 'file_format' to one of the supported types: 'parquet','orc','csv','json'. Invalid file: %s"
                error_msg = error_msg % (last_valid_type.value, file)
                raise Exception(error_msg)

            last_valid_type = self._type

        self._uri = str(path.parent)
        self._files = list_input


class DataSource:

    def __init__(self, client, table_name, descriptor, **kwargs):
        self._client = client
        self._table_name = table_name
        self._descriptor = descriptor

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
            return self._load_csv(**kwargs)
        elif type == Type.parquet:
            return self._load_parquet()
        elif type == Type.json:
            return self._load_json(**kwargs)
        elif type == Type.orc:
            return self._load_orc(**kwargs)
        elif type == Type.dask_cudf:
            return self._load_dask_cudf(**kwargs)
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

        return self._load_cudf_df(cudf_df)

    def _load_arrow_table(self, arrow_table):
        pandas_df = arrow_table.to_pandas()

        return self._load_pandas_df(pandas_df)

    def _load_result_set(self, result_set):
        cudf_df = result_set.columns

        return self._load_cudf_df(cudf_df)

    def _load_distributed_result_set(self, distributed_result_set):
        print(distributed_result_set)
        return_result = internal_api.create_table(
            self._client,
            self._table_name,
            type = internal_api.FileSchemaType.DISTRIBUTED,
            resultToken = distributed_result_set[0].resultToken
        )

        return self._create_table_status_to_bool(return_result)

    def _load_csv(self, **kwargs):
        # TODO percy manage datasource load errors
        if self._descriptor.files() == None:
            return False

        csv_args = internal_api._CsvArgs(self._descriptor.files(), **kwargs)
        csv_args.validation()

        return_result = internal_api.create_table(
            self._client,
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

    def _load_json(self, **kwargs):
        # TODO percy manage datasource load errors
        if self._descriptor.files() == None:
            return False

        json_lines = kwargs.get('json_lines', True)

        return_result = internal_api.create_table(
            self._client,
            self._table_name,
            type = internal_api.FileSchemaType.JSON,
            path = self._descriptor.files(),
            lines = json_lines
        )

        return self._create_table_status_to_bool(return_result)

    def _load_orc(self, **kwargs):
        # TODO percy manage datasource load errors
        if self._descriptor.files() == None:
            return False

        orc_args = internal_api._OrcArgs(**kwargs)
        orc_args.validation()

        return_result = internal_api.create_table(
            self._client,
            self._table_name,
            type = internal_api.FileSchemaType.ORC,
            path = self._descriptor.files(),
            orc_args = orc_args
        )

        return self._create_table_status_to_bool(return_result)

    def _load_dask_cudf(self, **kwargs):
        dask_client = kwargs['dask_client']
        dask_cudf = kwargs['dask_cudf']

        column_names = list(dask_cudf.dtypes.index)
        column_dtypes = [internal_api.get_np_dtype_to_gdf_dtype(x)
                         for x in dask_cudf.dtypes]

        return_result = internal_api.create_table(
            self._client,
            self._table_name,
            type=internal_api.FileSchemaType.DASK,
            names=column_names,
            dtypes=column_dtypes,
            dask_cudf=dask_cudf,
            dask_client=dask_client
        )

        return self._create_table_status_to_bool(return_result)

# BEGIN DataSource utils


def scan_datasource(client, directory, wildcard):
    files = internal_api.scan_datasource(client, directory, wildcard)
    return files


def build_datasource(client, input, table_name, **kwargs):
    ds = None

    file_format = kwargs.get('file_format', None)
    descriptor = Descriptor(input, file_format)

    if descriptor.is_cudf():
        ds = DataSource(None, table_name, descriptor, cudf_df = input)
    elif descriptor.is_pandas():
        ds = DataSource(None, table_name, descriptor, pandas_df = input)
    elif descriptor.is_arrow():
        ds = DataSource(None, table_name, descriptor, arrow_table = input)
    elif descriptor.is_result_set():
        ds = DataSource(None, table_name, descriptor, result_set = input)
    elif descriptor.is_distributed_result_set():
        ds = DataSource(None, table_name, descriptor, result_set = input.metaToken)
    elif descriptor.is_parquet() or descriptor.is_csv() or descriptor.is_json() or descriptor.is_orc():
        ds = DataSource(client, table_name, descriptor, **kwargs)
    elif descriptor.is_dask_cudf():
        ds = DataSource(client,
                        table_name,
                        descriptor,
                        dask_cudf=input,
                        **kwargs)

    # TODO percy raise error if ds is None

    return ds

# END DataSource utils
