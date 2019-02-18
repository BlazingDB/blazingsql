from enum import IntEnum


class Type(IntEnum):
    cudf = 0
    pandas = 1
    arrow = 2
    csv = 3
    parquet = 4


class DataSource:

    def __init__(self, path, filetype, **kwargs):
        self.path = path
        self.filetype = filetype
        self.is_valid = self._load()

    def is_valid(self):
        return self.is_valid

    # cudf.DataFrame in-gpu-memory
    def is_cudf(self):
        return self.filetype == FileType.cudf

    # pandas.DataFrame in-memory
    def is_pandas(self):
        return self.filetype == FileType.pandas

    # arrow file on filesystem or arrow in-memory
    def is_arrow(self):
        return self.filetype == FileType.arrow

    # csv file on filesystem
    def is_csv(self):
        return self.filetype == FileType.csv

    # parquet file on filesystem
    def is_parquet(self):
        return self.filetype == FileType.parquet

    def _load(self, **kwargs):
        # TODO percy compare against bz proto Status_Success or Status_Error
        # here we need to use low level api pyblazing.create_table
        return True

# BEGIN DataSource builders


def from_cudf():
    pass


def from_pandas():
    pass


def from_arrow():
    pass


# path (with wildcard support) is file system transparent
def from_csv(path, **kwargs):
    dstype = Type.csv
    ds = DataSource(path, filetype, kwargs)


# path (with wildcard support) is file system transparent
def from_parquet(path, **kwargs):
    dstype = Type.parquet
    ds = DataSource(path, filetype, kwargs)

# END DataSource builders
