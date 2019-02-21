from enum import IntEnum

import pyblazing
from pyblazing import SchemaFrom


class Type(IntEnum):
    cudf = 0
    pandas = 1
    arrow = 2
    csv = 3
    parquet = 4


class DataSource:

    def __init__(self, client, type, **kwargs):
        self.client = client

        self.type = type

        # declare all the possible fields (will be defined in _load)
        self.cudf_df = None
        self.pandas_df = None
        self.arrow_table = None
        self.csv = None
        self.parquet = None
        self.path = None

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
            Type.parquet: 'parquet'
        }

        # TODO percy path and stuff

        return type_str[self.type]

    def is_valid(self):
        return self.valid

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

    def dataframe(self):
        # TODO percy add more support
        if type == Type.cudf:
            return self.cudf_df
        elif type == Type.csv:
            return self.csv.columns
        elif type == Type.parquet:
            return self.parquet.columns

        return None

    def _load(self, type, **kwargs):
        if type == Type.cudf:
            cudf_df = kwargs.get('cudf_df', None)
            return self._load_cudf_df(cudf_df)
        elif type == Type.pandas:
            pandas_df = kwargs.get('pandas_df', None)
            return self._load_pandas_df(pandas_df)
        elif type == Type.arrow:
            arrow_table = kwargs.get('arrow_table', None)
            return self._load_arrow_table(arrow_table)
        elif type == Type.csv:
            table_name = kwargs.get('table_name', None)
            path = kwargs.get('path', None)
            return self._load_csv(table_name, path)
        elif type == Type.parquet:
            table_name = kwargs.get('table_name', None)
            path = kwargs.get('path', None)
            return self._load_parquet(table_name, path)
        else:
            # TODO percy manage errors
            raise Exception("invalid datasource type")

        # TODO percy compare against bz proto Status_Success or Status_Error
        # here we need to use low level api pyblazing.create_table
        return False

    def _load_cudf_df(self, cudf_df):
        # TODO percy manage datasource load errors
        if cudf_df == None:
            return False

        self.cudf_df = cudf_df

        # TODO percy see if we need to perform sanity check for cudf_df object
        self.valid = True

        return self.valid

    def _load_pandas_df(self, pandas_df):
        # TODO percy manage datasource load errors
        if pandas_df == None:
            return False

        self.pandas_df = pandas_df

        # TODO percy see if we need to perform sanity check for pandas_df object
        self.valid = True

        return self.valid

    def _load_arrow_table(self, arrow_table):
        # TODO percy manage datasource load errors
        if arrow_table == None:
            return False

        self.arrow_table = arrow_table

        # TODO percy see if we need to perform sanity check for arrow_table object
        self.valid = True

        return self.valid

    def _load_csv(self, table_name, path):
        # TODO percy
        pass

    def _load_parquet(self, table_name, path):
        # TODO percy manage datasource load errors
        if path == None:
            return False

        self.path = path

        table = pyblazing.create_table(
            self.client,
            table_name = table_name,
            type = SchemaFrom.ParquetFile,
            path = path
        )

        self.parquet = table

        # TODO percy see if we need to perform sanity check for arrow_table object
        self.valid = True

        return self.valid

# BEGIN DataSource builders


def from_cudf(cudf_df):
    return DataSource(Type.cudf, cudf_df = cudf_df)


def from_pandas(pandas_df):
    return DataSource(Type.pandas, pandas_df = pandas_df)


def from_arrow(arrow_table):
    return DataSource(Type.arrow, arrow_table = arrow_table)


# TODO percy path (with wildcard support) is file system transparent
def from_csv(client, table_name, path):
    return DataSource(client, Type.csv, table_name = table_name, path = path)


# TODO percy path (with wildcard support) is file system transparent
def from_parquet(client, table_name, path):
    return DataSource(client, Type.parquet, table_name = table_name, path = path)

# END DataSource builders
