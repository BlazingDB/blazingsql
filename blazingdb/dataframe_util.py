from pygdf.dataframe import Series, DataFrame
import collections
import pycuda.driver as drv



class gdfHandles:
    
    def _init_(self, dataHandle, validHandle, size, null_count, dtype, dtype_info, col_name, table_name = None):
        
        self._dataHandle = dataHandle
        self._validHandle = validHandle
        self._size = size
        self._null_count = null_count
        self._dtype = dtype
        self._dtype_info = dtype_info
        self._col_name = col_name
        self._table_name = table_name     




class DataFrameParser:
    
    def getColumnNames(self, df):
        
        return list(df._cols.keys())
    
    
    def getGdfs(self, df):
        
        gdfs = []
        for name, series in df._cols.items():
            gdfs.append(series._column.cffi_view)
            
        return gdfs
    
            
    def getGdfHandles(self, df):
        
        dev = drv.Device(0)
        
        gdfHandless = []
        for name, series in df._cols.items():
#             WSM TODO add if statement for valid != nullptr
            gdfHandless.append(gdfHandles(
                drv.mem_get_ipc_handle(series._column.cffi_view.data),
                drv.mem_get_ipc_handle(series._column.cffi_view.valid),
                series._column.cffi_view.size,
                series._column.cffi_view.null_count,
                series._column.cffi_view.dtype,
                series._column.cffi_view.dtype_info,
                name,
                None))
            
        return gdfHandles
                
                
        
        