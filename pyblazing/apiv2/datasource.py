from enum import IntEnum


class Type(IntEnum):
    cudf = 0
    pandas = 1
    arrow = 2
    csv = 3
    parquet = 4


class DataSource:

    def __init__(self, type, **kwargs):
        self.type = type

        # declare all the possible fields (will be defined in _load)
        self.cudf_df = None
        self.pandas_df = None
        self.arrow_table = None
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
        return self.cudf_df

    def _load(self, type, **kwargs):
        if type == Type.cudf:
            cudf_df = kwargs.get('cudf_df', None)
            return self._load_cudf_df(cudf_df)
        elif type == Type.pandas:
            pandas_df = kwargs.get('pandas_df', None)
            return self._load_cudf_df(pandas_df)
        elif type == Type.arrow:
            arrow_table = kwargs.get('arrow_table', None)
            return self._load_arrow_table(arrow_table)
        elif type == Type.csv:
            self.path = kwargs.get('path', None)

            pass
        elif type == Type.parquet:
            self.path = kwargs.get('path', None)

            pass
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

# BEGIN DataSource builders


def from_cudf(cudf_df):
    return DataSource(Type.cudf, cudf_df = cudf_df)


def from_pandas(pandas_df):
    return DataSource(Type.pandas, pandas_df = pandas_df)


def from_arrow(arrow_table):
    return DataSource(Type.arrow, arrow_table = arrow_table)


# path (with wildcard support) is file system transparent
def from_csv(path, **kwargs):
    dstype = Type.csv
    ds = DataSource(path, filetype, kwargs)


# path (with wildcard support) is file system transparent
def from_parquet(path, **kwargs):
    dstype = Type.parquet
    ds = DataSource(path, filetype, kwargs)

# END DataSource builders
