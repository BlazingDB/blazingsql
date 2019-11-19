from enum import IntEnum, unique

# NOTE Same values from ral (bsqlengine) DataType (DataType.h and cio.pdx)
@unique
class DataType(IntEnum):
    UNDEFINED = 999,
    PARQUET = 0,
    ORC = 1,
    CSV = 2,
    JSON = 3,
    CUDF = 4,
    DASK_CUDF = 5
