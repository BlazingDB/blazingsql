# distutils: language = c++
# cio.pxd

from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.pair cimport pair
from libcpp.map cimport map
from libcpp.memory cimport shared_ptr

from libcpp cimport bool
from pyarrow.lib cimport *
from cudf._libxx.cpp cimport *

from cudf import DataFrame

from cudf._libxx.table cimport table

from libc.stdint cimport (  # noqa: E211
    uint8_t,
    uint32_t,
    int64_t,
    int32_t,
    int16_t,
    int8_t,
    uintptr_t
)


cdef extern from "../include/engine/errors.h":
    cdef void raiseInitializeError()
    cdef void raiseFinalizeError()
    cdef void raiseRunQueryError()
    cdef void raiseParseSchemaError()
    cdef void raiseRegisterFileSystemHDFSError();
    cdef void raiseRegisterFileSystemGCSError();
    cdef void raiseRegisterFileSystemS3Error();
    cdef void raiseRegisterFileSystemLocalError();


from cudf._libxx.cpp.column cimport *
from cudf._libxx.cpp.column.column_view cimport *
from cudf._libxx.cpp.types cimport *
from cudf._libxx.cpp.table cimport *
from cudf._libxx.cpp.table.table_view cimport *

ctypedef column_view CudfColumnView
ctypedef table_view CudfTableView
ctypedef table CudfTable


cdef extern from "cudf/types.hpp" namespace "cudf" nogil:
    ctypedef uint8_t       valid_type

cdef extern from "cudf/cudf.h" nogil:

    ctypedef int8_t        gdf_bool8
    ctypedef int64_t       gdf_date64
    ctypedef int32_t       gdf_date32
    ctypedef int64_t       gdf_timestamp
    ctypedef int32_t       gdf_category
    ctypedef int32_t       gdf_nvstring_category

    ctypedef enum gdf_dtype:
        GDF_invalid=0,
        GDF_INT8,
        GDF_INT16,
        GDF_INT32,
        GDF_INT64,
        GDF_FLOAT32,
        GDF_FLOAT64,
        GDF_BOOL8,
        GDF_DATE32,
        GDF_DATE64,
        GDF_TIMESTAMP,
        GDF_CATEGORY,
        GDF_STRING,
        GDF_STRING_CATEGORY,
        N_GDF_TYPES,

    ctypedef enum gdf_error:
        GDF_SUCCESS=0,
        GDF_CUDA_ERROR,
        GDF_UNSUPPORTED_DTYPE,
        GDF_COLUMN_SIZE_MISMATCH,
        GDF_COLUMN_SIZE_TOO_BIG,
        GDF_DATASET_EMPTY,
        GDF_VALIDITY_MISSING,
        GDF_VALIDITY_UNSUPPORTED,
        GDF_INVALID_API_CALL,
        GDF_JOIN_DTYPE_MISMATCH,
        GDF_JOIN_TOO_MANY_COLUMNS,
        GDF_DTYPE_MISMATCH,
        GDF_UNSUPPORTED_METHOD,
        GDF_INVALID_AGGREGATOR,
        GDF_INVALID_HASH_FUNCTION,
        GDF_PARTITION_DTYPE_MISMATCH,
        GDF_HASH_TABLE_INSERT_FAILURE,
        GDF_UNSUPPORTED_JOIN_TYPE,
        GDF_C_ERROR,
        GDF_FILE_ERROR,
        GDF_MEMORYMANAGER_ERROR,
        GDF_UNDEFINED_NVTX_COLOR,
        GDF_NULL_NVTX_NAME,
        GDF_NOTIMPLEMENTED_ERROR,
        N_GDF_ERRORS

    ctypedef enum gdf_time_unit:
        TIME_UNIT_NONE=0
        TIME_UNIT_s,
        TIME_UNIT_ms,
        TIME_UNIT_us,
        TIME_UNIT_ns

    ctypedef struct gdf_dtype_extra_info:
        gdf_time_unit time_unit
        void *category

    ctypedef struct gdf_column:
        void *data
        valid_type *valid
        size_type size
        gdf_dtype dtype
        size_type null_count
        gdf_dtype_extra_info dtype_info
        char *col_name

    ctypedef union gdf_data:
       int8_t        si08
       int16_t       si16
       int32_t       si32
       int64_t       si64
       float         fp32
       double        fp64
       gdf_bool8      b08
       gdf_date32    dt32
       gdf_date64    dt64
       gdf_timestamp tmst

    ctypedef struct gdf_scalar:
      gdf_data  data
      gdf_dtype dtype
      bool      is_valid


ctypedef gdf_column* gdf_column_ptr


cdef extern from "cudf/legacy/io_types.hpp":
    #ctypedef enum quote_style:
    #    QUOTE_MINIMAL,
    #    QUOTE_ALL,
    #    QUOTE_NONNUMERIC,
    #    QUOTE_NONE
    cdef struct csv_read_arg:
        string compression
        char lineterminator
        char delimiter
        bool windowslinetermination
        bool delim_whitespace
        bool skipinitialspace
        bool skip_blank_lines
        size_type nrows
        size_type skiprows
        size_type skipfooter
        size_type header
        vector[string] names
        vector[string] dtype
        vector[int] use_cols_indexes
        vector[string] use_cols_names
        vector[string] true_values
        vector[string] false_values
        vector[string] na_values
        bool keep_default_na
        bool na_filter
        string prefix
        bool mangle_dupe_cols
        bool dayfirst
        char thousands
        char decimal
        char comment
        char quotechar
        #quote_style quoting
        bool doublequote
        size_t byte_range_offset
        size_t byte_range_size
        gdf_time_unit out_time_unit
    cdef struct source_info:
        source_info(string filepath)
    cdef struct json_read_arg:
        vector[string] dtype
        string compression
        bool lines
        size_t byte_range_offset
        size_t byte_range_size
        json_read_arg(source_info source)
    cdef struct orc_read_arg:
        vector[string] columns
        int stripe
        int skip_rows
        int num_rows
        bool use_index
        bool use_np_dtypes
        gdf_time_unit timestamp_unit

ctypedef gdf_scalar* gdf_scalar_ptr

cdef extern from "../include/io/io.h":
    cdef struct ResultSet:
        unique_ptr[table] cudfTable
        vector[string]  names
        bool skipdata_analysis_fail
    
    ctypedef enum DataType:
        UNDEFINED = 999,
        PARQUET = 0,
        ORC = 1,
        CSV = 2,
        JSON = 3,
        CUDF = 4,
        DASK_CUDF = 5,
        ARROW = 6
    cdef struct ReaderArgs:
        orc_read_arg orcReaderArg
        json_read_arg jsonReaderArg
        csv_read_arg csvReaderArg
    cdef struct TableSchema:
        BlazingTableView blazingTableView
        vector[type_id] types
        vector[string]  names
        vector[string]  files
        vector[string] datasource
        vector[unsigned long] calcite_to_file_indices
        vector[bool] in_file
        int data_type
        ReaderArgs args
        BlazingTableView metadata
        vector[vector[int]] row_groups_ids
        shared_ptr[CTable] arrow_table
    cdef struct HDFS:
        string host
        int port
        string user
        short DriverType
        string kerberosTicket
    cdef struct S3:
        string bucketName
        short encryptionType
        string kmsKeyAmazonResourceName
        string accessKeyId
        string secretKey
        string sessionToken
    cdef struct GCS:
        string projectId
        string bucketName
        bool useDefaultAdcJsonFile
        string adcJsonFile
    pair[bool, string] registerFileSystemHDFS(HDFS hdfs, string root, string authority) except +raiseRegisterFileSystemHDFSError
    pair[bool, string] registerFileSystemGCS( GCS gcs, string root, string authority) except +raiseRegisterFileSystemGCSError
    pair[bool, string] registerFileSystemS3( S3 s3, string root, string authority) except +raiseRegisterFileSystemS3Error
    pair[bool, string] registerFileSystemLocal(  string root, string authority) except +raiseRegisterFileSystemLocalError
    TableSchema parseSchema(vector[string] files, string file_format_hint, vector[string] arg_keys, vector[string] arg_values, vector[pair[string,type_id]] types) except +raiseParseSchemaError
    unique_ptr[ResultSet] parseMetadata(vector[string] files, pair[int,int] offsets, TableSchema schema, string file_format_hint, vector[string] arg_keys, vector[string] arg_values) except +raiseParseSchemaError

cdef extern from "../src/execution_graph/logic_controllers/LogicPrimitives.h" namespace "ral::frame":
        cdef cppclass BlazingTable:
            size_type num_columns
            size_type num_rows
            CudfTableView view()
            vector[string] names()

        cdef cppclass BlazingTableView:
            BlazingTableView()
            BlazingTableView(CudfTableView, vector[string]) except +
            CudfTableView view()
            vector[string] names()

# REMARK: We have some compilation errors from cython assigning temp = unique_ptr[ResultSet]
# We force the move using this function
cdef extern from * namespace "blazing":
        """
        namespace blazing {
        template <class T> inline typename std::remove_reference<T>::type&& blaz_move(T& t) { return std::move(t); }
        template <class T> inline typename std::remove_reference<T>::type&& blaz_move(T&& t) { return std::move(t); }
        }
        """
        cdef T blaz_move[T](T)

cdef extern from "../include/engine/engine.h":

        unique_ptr[ResultSet] performPartition(int masterIndex, vector[NodeMetaDataTCP] tcpMetadata, int ctxToken, BlazingTableView blazingTableView, vector[string] columnNames) except +raiseRunQueryError

        cdef struct NodeMetaDataTCP:
            string ip
            int communication_port
        unique_ptr[ResultSet] runQuery(int masterIndex, vector[NodeMetaDataTCP] tcpMetadata, vector[string] tableNames, vector[TableSchema] tableSchemas, vector[vector[string]] tableSchemaCppArgKeys, vector[vector[string]] tableSchemaCppArgValues, vector[vector[string]] filesAll, vector[int] fileTypes, int ctxToken, string query, unsigned long accessToken, vector[vector[map[string,string]]] uri_values_cpp, bool use_execution_graph) except +raiseRunQueryError
        unique_ptr[ResultSet] runSkipData(BlazingTableView metadata, vector[string] all_column_names, string query) except +raiseRunQueryError

        cdef struct TableScanInfo:
            vector[string] relational_algebra_steps
            vector[string] table_names
            vector[vector[int]] table_columns
        TableScanInfo getTableScanInfo(string logicalPlan)

cdef extern from "../include/engine/initialize.h":
    cdef void initialize(int ralId, int gpuId, string network_iface_name, string ralHost, int ralCommunicationPort, bool singleNode) except +raiseInitializeError
    cdef void finalize() except +raiseFinalizeError
