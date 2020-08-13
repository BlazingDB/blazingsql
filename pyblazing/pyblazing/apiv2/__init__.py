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
    ARROW = 6


# NOTE Same values from io
@unique
class S3EncryptionType(IntEnum):
    UNDEFINED = 0
    NONE = 1
    AES_256 = 2
    AWS_KMS = 3


