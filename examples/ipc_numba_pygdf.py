# Sample source code from the Tutorial Introduction in the documentation.
import blazingdb.protocol
import blazingdb.protocol.interpreter
import blazingdb.protocol.orchestrator
import blazingdb.protocol.transport.channel

import multiprocessing as mp

from libgdf_cffi import ffi

from pygdf import _gdf
from pygdf import column
from pygdf import numerical
from pygdf import DataFrame
from pygdf.dataframe import Series
from pygdf.buffer import Buffer

from numba import cuda
import numpy as np
import pandas as pd

import utils


unix_path = '/tmp/demo.socket'


def gen_data_frame(nelem):
    pdf = pd.DataFrame()
    pdf['data'] = np.arange(nelem, dtype=np.int32)
    pdf['valid'] = np.arange(nelem, dtype=np.int8)

    df = DataFrame.from_pandas(pdf)
    return df

def get_ipc_handle_for(df):
    ipch_handle = None
    cffiView = df._column.cffi_view
    print('dtype')
    print(cffiView.dtype)
    ipch_handle = df._column._data.mem.get_ipc_handle()
    return ipch_handle

def client():
    sample_df = gen_data_frame(25)

    ipch = get_ipc_handle_for(sample_df['data'])
    hb = bytes(ipch._ipc_handle.handle)

    connection = blazingdb.protocol.UnixSocketConnection(unix_path)
    client = blazingdb.protocol.Client(connection)
    res = client.send(hb)
    print(res)
    print("done wait")


def server():
    print('waiting')

    connection = blazingdb.protocol.UnixSocketConnection(unix_path)
    server = blazingdb.protocol.Server(connection)

    def from_cffi_view(cffi_view):
        data_mem, mask_mem = _gdf.cffi_view_to_column_mem(cffi_view)
        data_buf = Buffer(data_mem)
        mask = None
        return column.Column(data=data_buf, mask=mask)

    def get_column(ipch):
        with cuda.open_ipc_array(ipch, shape=25, dtype=np.int32) as data_ptr:
            data = _gdf.unwrap_devary(data_ptr)
            print(type(data))

            print("here is the host pointer")
            print(type(data_ptr.copy_to_host()))
            print(data_ptr.copy_to_host())

            print('gpu array')
            gdf_col = _gdf.columnview_from_devary(data_ptr)
            newcol = from_cffi_view(gdf_col)
            return newcol.copy()

    def controller(ipch):
        newcol = get_column(ipch)
        outcols = []
        outcols.append(newcol.view(numerical.NumericalColumn, dtype=newcol.dtype))

        DF = DataFrame()
        DF["colA"] = outcols[0]
        print(DF)

        return b'hi back!'

    server.handle(controller)


import time

if __name__ == '__main__':
    p1 = mp.Process(target=client)
    p2 = mp.Process(target=server)
    p2.start()
    time.sleep(0.5)
    p1.start()
