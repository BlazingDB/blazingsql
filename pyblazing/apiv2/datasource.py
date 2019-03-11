from enum import IntEnum

from .bridge import internal_api

# TODO BIG TODO percy blazing-io didn't expose the wildcard API of FileSystem API


# TODO percy fix this we need to define a unify type system for pyblazing
def get_dtype_values(dtypes):
    values = []

    def gdf_type(type_name):
        dicc = {
            'str': libgdf.GDF_STRING,
            'date': libgdf.GDF_DATE64,
            'date64': libgdf.GDF_DATE64,
            'date32': libgdf.GDF_DATE32,
            'timestamp': libgdf.GDF_TIMESTAMP,
            'category': libgdf.GDF_CATEGORY,
            'float': libgdf.GDF_FLOAT32,
            'double': libgdf.GDF_FLOAT64,
            'float32': libgdf.GDF_FLOAT32,
            'float64': libgdf.GDF_FLOAT64,
            'short': libgdf.GDF_INT16,
            'long': libgdf.GDF_INT64,
            'int': libgdf.GDF_INT32,
            'int32': libgdf.GDF_INT32,
            'int64': libgdf.GDF_INT64,
        }
        if dicc.get(type_name):
            return dicc[type_name]
        return libgdf.GDF_INT64

    for key in dtypes:
        values.append(gdf_type(dtypes[key]))

    print('>>>> dtyps for', dtypes.values())
    print(values)
    return values


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

        if self.type == Type.cudf:
            return self.cudf_df
        elif self.type == Type.csv:
            return self.csv.columns
        elif self.type == Type.parquet:
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
            column_names = kwargs.get('column_names', None)
            column_types = kwargs.get('column_types', None)
            delimiter = kwargs.get('delimiter', '|')
            skip_rows = kwargs.get('skip_rows', 0)
            return self._load_csv(table_name, path, column_names, column_types, delimiter, skip_rows)
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

    def _load_csv(self, table_name, path, column_names, column_types, delimiter, skip_rows):
        # TODO percy manage datasource load errors
        if path == None:
            return False

        self.path = path

        print("NOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO")

        table = internal_api.register_table_schema(
            self.client,
            table_name = table_name,
            type = internal_api.SchemaFrom.CsvFile,
            path = path,
            delimiter = delimiter,
            names = column_names,
            dtypes = column_types,
            skip_rows = skip_rows
        )

        print(table)

        print("EENDDDDDDDDDDDDDDDD")

        self.parquet = table

        # TODO percy see if we need to perform sanity check for arrow_table object
        self.valid = True

        return self.valid

    def _load_parquet(self, table_name, path):
        # TODO percy manage datasource load errors
        if path == None:
            return False

        self.path = path

        print("NOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO")

        table = internal_api.register_table_schema(
            self.client,
            table_name = table_name,
            type = internal_api.SchemaFrom.ParquetFile,
            path = path
        )

        print(table)

        print("EENDDDDDDDDDDDDDDDD")

        self.parquet = table

        # TODO percy see if we need to perform sanity check for arrow_table object
        self.valid = True

        return self.valid

# BEGIN DataSource builders


def from_cudf(cudf_df):
    return DataSource(None, Type.cudf, cudf_df = cudf_df)


def from_pandas(pandas_df):
    return DataSource(None, Type.pandas, pandas_df = pandas_df)


def from_arrow(arrow_table):
    return DataSource(None, Type.arrow, arrow_table = arrow_table)


def from_csv(client, table_name, path, column_names, column_types, delimiter, skip_rows):
    return DataSource(client, Type.csv,
        table_name = table_name,
        path = path,
        column_names = column_names,
        column_types = column_types,
        delimiter = delimiter,
        skip_rows = skip_rows
    )


# TODO percy path (with wildcard support) is file system transparent
def from_parquet(client, table_name, path):
    return DataSource(client, Type.parquet, table_name = table_name, path = path)

# END DataSource builders
