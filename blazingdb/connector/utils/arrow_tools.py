# coding=utf-8

from StringIO import StringIO
from pyarrow import open_stream


def to_arrow_stream_reader(arrow_bytes):
    """ pyarrow.RecordBatchStreamReader """
    io_bytes = StringIO(arrow_bytes)
    arrow_stream_reader = open_stream(io_bytes)
    return arrow_stream_reader


def to_arrow_table(arrow_bytes):
    """ pyarrow.Table """
    arrow_reader = to_arrow_stream_reader(arrow_bytes)
    arrow_table = arrow_reader.read_all()
    return arrow_table
