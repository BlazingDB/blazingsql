# distutils: language = c++
# cio.pxd

from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.pair cimport pair
from libcpp.map cimport map
from libcpp.memory cimport shared_ptr
from cudf._lib.column cimport Column
from libcpp.utility cimport pair
from libcpp cimport bool
from pyarrow.lib cimport *

from cudf import DataFrame
from cudf._lib.cpp.types cimport type_id
from cudf._lib.table cimport table

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
    cdef void raiseBlazingSetAllocatorError()
    cdef void raiseGetProductDetailsError()
    cdef void raisePerformPartitionError()
    cdef void raiseRunGenerateGraphError()
    cdef void raiseRunExecuteGraphError()
    cdef void raiseRunSkipDataError()
    cdef void raiseParseSchemaError()
    cdef void raiseRegisterFileSystemHDFSError()
    cdef void raiseRegisterFileSystemGCSError()
    cdef void raiseRegisterFileSystemS3Error()
    cdef void raiseRegisterFileSystemLocalError()


from cudf._lib.cpp.column cimport *
from cudf._lib.cpp.column.column_view cimport *
from cudf._lib.cpp.types cimport *
from cudf._lib.cpp.table cimport *
from cudf._lib.cpp.table.table_view cimport *

ctypedef column_view CudfColumnView
ctypedef table_view CudfTableView
ctypedef table CudfTable


cdef extern from "../include/io/io.h" nogil:
    cdef struct ResultSet:
        unique_ptr[table] cudfTable
        vector[string]  names
        bool skipdata_analysis_fail


    cdef struct PartitionedResultSet:
        vector[unique_ptr[table]] cudfTables
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


    cdef struct TableSchema:
        vector[BlazingTableView] blazingTableViews
        vector[type_id] types
        vector[string]  names
        vector[string]  files
        vector[string] datasource
        vector[unsigned long] calcite_to_file_indices
        vector[bool] in_file
        int data_type
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
        string endpointOverride
        string region


    cdef struct GCS:
        string projectId
        string bucketName
        bool useDefaultAdcJsonFile
        string adcJsonFile


    pair[bool, string] registerFileSystemHDFS(HDFS hdfs, string root, string authority) except +raiseRegisterFileSystemHDFSError
    pair[bool, string] registerFileSystemGCS( GCS gcs, string root, string authority) except +raiseRegisterFileSystemGCSError
    pair[bool, string] registerFileSystemS3( S3 s3, string root, string authority) except +raiseRegisterFileSystemS3Error
    pair[bool, string] registerFileSystemLocal(  string root, string authority) except +raiseRegisterFileSystemLocalError
    TableSchema parseSchema(vector[string] files, string file_format_hint, vector[string] arg_keys, vector[string] arg_values, vector[pair[string,type_id]] types, bool ignore_missing_paths) except +raiseParseSchemaError
    unique_ptr[ResultSet] parseMetadata(vector[string] files, pair[int,int] offsets, TableSchema schema, string file_format_hint, vector[string] arg_keys, vector[string] arg_values) except +raiseParseSchemaError


cdef extern from "../src/execution_graph/logic_controllers/LogicPrimitives.h" namespace "ral::frame":
        cdef cppclass BlazingTable:
            BlazingTable(unique_ptr[CudfTable] table, const vector[string] & columnNames)
            BlazingTable(const CudfTableView & table, const vector[string] & columnNames)
            size_type num_columns
            size_type num_rows
            CudfTableView view()
            vector[string] names()
            void ensureOwnership()
            unique_ptr[CudfTable] releaseCudfTable()

        cdef cppclass BlazingTableView:
            BlazingTableView()
            BlazingTableView(CudfTableView, vector[string]) except +
            CudfTableView view()
            vector[string] names()

cdef extern from "../src/execution_graph/logic_controllers/taskflow/graph.h" namespace "ral::cache":
        cdef cppclass graph:
            shared_ptr[CacheMachine] get_kernel_output_cache(size_t kernel_id, string cache_id) except +
            void set_input_and_output_caches(shared_ptr[CacheMachine] input_cache, shared_ptr[CacheMachine] output_cache)

cdef extern from "../src/execution_graph/logic_controllers/CacheMachine.h" namespace "ral::cache":
        cdef cppclass CacheData
        cdef unique_ptr[GPUCacheDataMetaData] cast_cache_data_to_gpu_with_meta(unique_ptr[CacheData] base_pointer)
        cdef cppclass MetadataDictionary:
            void set_values(map[string,string] new_values)
            map[string,string] get_values()
            void print() nogil
        cdef cppclass GPUCacheDataMetaData:
            GPUCacheDataMetaData(BlazingTable table, MetadataDictionary metadata)
            map[string, string] get_map()
            pair[unique_ptr[BlazingTable], MetadataDictionary ] decacheWithMetaData()
        cdef cppclass CacheMachine:
            void addCacheData(unique_ptr[CacheData] cache_data, const string & message_id, bool always_add ) nogil except +
            void addToCache(unique_ptr[BlazingTable] table, const string & message_id , bool always_add) nogil except+
            unique_ptr[CacheData] pullCacheData() nogil  except +
            unique_ptr[CacheData] pullCacheData(string message_id) nogil except +
            bool has_next_now() except +

# REMARK: We have some compilation errors from cython assigning temp = unique_ptr[ResultSet]
# We force the move using this function
cdef extern from * namespace "blazing":
        """
        namespace blazing {
        template <class T> inline typename std::remove_reference<T>::type&& blaz_move(T& t) { return std::move(t); }
        template <class T> inline typename std::remove_reference<T>::type&& blaz_move(T&& t) { return std::move(t); }
        template <class T> inline typename std::remove_reference<T>::type&& blaz_move2(T& t) { return std::move(t); }
        template <class T> inline typename std::remove_reference<T>::type&& blaz_move2(T&& t) { return std::move(t); }
        }
        """
        cdef T blaz_move[T](T) nogil

        cdef unique_ptr[CacheData] blaz_move2(unique_ptr[GPUCacheDataMetaData]) nogil

cdef extern from "../include/engine/engine.h" nogil:

        unique_ptr[ResultSet] performPartition(int masterIndex, vector[NodeMetaDataTCP] tcpMetadata, int ctxToken, BlazingTableView blazingTableView, vector[string] columnNames) except +raisePerformPartitionError

        cdef struct NodeMetaDataTCP:
            string worker_id
            string ip
            int communication_port
        shared_ptr[graph] runGenerateGraph(int masterIndex, vector[NodeMetaDataTCP] tcpMetadata, vector[string] tableNames, vector[string] tableScans, vector[TableSchema] tableSchemas, vector[vector[string]] tableSchemaCppArgKeys, vector[vector[string]] tableSchemaCppArgValues, vector[vector[string]] filesAll, vector[int] fileTypes, int ctxToken, string query, unsigned long accessToken, vector[vector[map[string,string]]] uri_values_cpp, map[string,string] config_options) except +raiseRunGenerateGraphError
        unique_ptr[PartitionedResultSet] runExecuteGraph(shared_ptr[graph]) nogil except +raiseRunExecuteGraphError
        unique_ptr[ResultSet] runSkipData(BlazingTableView metadata, vector[string] all_column_names, string query) nogil except +raiseRunSkipDataError

        cdef struct TableScanInfo:
            vector[string] relational_algebra_steps
            vector[string] table_names
            vector[vector[int]] table_columns
        TableScanInfo getTableScanInfo(string logicalPlan)

cdef extern from "../include/engine/initialize.h":
    cdef pair[shared_ptr[CacheMachine], shared_ptr[CacheMachine] ] initialize(int ralId, string worker_id, int gpuId, string network_iface_name, string ralHost, int ralCommunicationPort, bool singleNode, map[string,string] config_options) nogil except +raiseInitializeError
    cdef void finalize() nogil except +raiseFinalizeError
    cdef void blazingSetAllocator(string allocation_mode, size_t initial_pool_size, map[string,string] config_options) nogil except +raiseBlazingSetAllocatorError

cdef extern from "../include/engine/static.h" nogil:
    cdef map[string,string] getProductDetails() except +raiseGetProductDetailsError
