# BlazingSQL 21.08.00 (TBD)

## New Features


## Improvements
- #1571 Update ucx-py versions to 0.21
- #1554 return ok for filesystems
- #1572 Setting up default value for max_bytes_chunk_read to 256 MB


## Bug Fixes
- #1570 Fix build due to changes in rmm device buffer
- #1576 Fix `CC`/`CXX` variables in CI


# BlazingSQL 21.06.00 (June 10th, 2021)

## New Features
- #1471 Unbounded partitioned windows 
- #1445 Support for CURRENT_DATE, CURRENT_TIME and CURRENT_TIMESTAMP
- #1505 Support for right outer join
- #1523 Support for DURATION type
- #1552 Support for concurrency in E2E tests

## Improvements
- #1464 Better Support for unsigned types in C++ side
- #1511 Folder refactoring related to caches, kernels, execution_graph, BlazingTable
- #1522 Improve data loading when the algebra contains only BindableScan/Scan and Limit
- #1524 Enable support for spdlog 1.8.5
- #1547 Update RAPIDS version references
- #1539 Support ORDERing by null values
- #1551 Support for spdlog 1.8.5
- #1553 multiple columns inside COUNT() statement

## Bug Fixes
- #1455 Support for IS NOT FALSE condition
- #1502 Fix IS NOT DISTINCT FROM with joins
- #1475 Fix wrong results from timestampdiff/add
- #1528 Fixed build issues due to cudf aggregation API change 
- #1540 Comparing param set to true for e2e
- #1543 Enables provider unit_tests
- #1548 Fix orc statistic building
- #1550 Fix Decimal/Fixed Point issue
- #1519 Fix for max_bytes_chunk_read param to csv files
- #1559 Fix `ucx-py` versioning specs
- #1557 Reading chunks of max bytes for csv files


# BlazingSQL 0.19.0 (April 21, 2021)

## New Features
- #1367 OverlapAccumulator Kernel
- #1364 Implement the concurrent API (bc.sql with token, bc.status, bc.fetch)
- #1426 Window Functions without partitioning 
- #1349 Add e2e test for Hive Partitioned Data
- #1396 Create tables from other RDBMS
- #1427 Support for CONCAT alias operator
- #1424 Add get physical plan with explain
- #1472 Implement predicate pushdown for data providers

## Improvements
- #1325 Refactored CacheMachine.h and CacheMachine.cpp 
- #1322 Updated and enabled several E2E tests
- #1333 Fixing build due to cudf update
- #1344 Removed GPUCacheDataMetadata class
- #1376 Fixing build due to some strings refactor in cudf, undoing the replace workaround
- #1430 Updating GCP to >= version
- #1331 Added flag to enable null e2e testing
- #1418 Adding support for docker image
- #1434 Added documentation for C++ and Python in Sphinx
- #1419 Added concat cache machine timeout 
- #1444 Updating GCP to >= version
- #1349 Add e2e test for Hive Partitioned Data
- #1447 Improve getting estimated output num rows
- #1473 Added Warning to Window Functions
- #1482 Improve test script for blazingsql-testing-file
- #1480 Improve dependencies script
- #1433 Adding ArrowCacheData, refactoring CacheData files

## Bug Fixes
- #1335 Fixing uninitialized var in orc metadata and handling the parseMetadata exceptions properly
- #1339 Handling properly the nulls in case conditions with strings
- #1346 Delete allocated host chunks
- #1348 Capturing error messages due to exceptions properly
- #1350 Fixed bug where there are no projects in a bindable table scan
- #1359 Avoid cuda issues when free pinned memory
- #1365 Fixed build after sublibs changes on cudf
- #1369 Updated java path for powerpc build 
- #1371 Fixed e2e settings
- #1372 Recompute `columns_to_hash` in DistributeAggregationKernel
- #1375 Fix empty row_group_ids for parquet
- #1380 Fixed issue with int64 literal values 
- #1379 Remove ProjectRemoveRule
- #1389 Fix issue when CAST a literal
- #1387 Skip getting orc metadata for decimal type
- #1392 Fix substrings with nulls
- #1398 Fix performance regression
- #1401 Fix support for minus unary operation
- #1415 Fixed bug where num_batches was not getting set in BindableTableScan 
- #1413 Fix for null tests 13 and 23 of windowFunctionTest
- #1416 Fix full join when both tables contains nulls
- #1423 Fix temporary directory for hive partition test
- #1351 Fixed 'count distinct' related issues
- #1425 Fix for new joins API
- #1400 Fix for Column aliases when exists a Join op
- #1456 Raising exceptions on Python side for RAL
- #1466 SQL providers: update README.md
- #1470 Fix pre compiler flags for sql parsers
- #1504 Fixing some conflicts in Dockerfile

## Deprecated Features
- #1394 Disabled support for outer joins with inequalities 

# BlazingSQL 0.18.0 (February 24, 2021)

## New Features
- #1139 Adding centralized task executor for kernels
- #1200 Implement string REGEXP_REPLACE
- #1237 Added task memory management
- #1244 Added memory monitor ability to downgrade task data
- #1232 Update PartwiseJoin and JoinPartition kernel using the task executor internally
- #1238 Implements MergeStramKernel executor model
- #1259 Implements SortAndSamplernel executor model, also avoid setting up num of samples
- #1271 Added Hive utility for partitioned data
- #1289 Multiple concurrent query support 
- #1285 Infer PROTOCOL when Dask client is passed
- #1294 Add config options for logger
- #1301 Added usage of pinned buffers for communication and fixes various UCX related bugs
- #1298 Implement progress bar for run query (using tqdm)
- #1284 Initial support for Windows Function
- #1303 Add support for INITCAP
- #1313 getting and using ORC metadata
- #1347 Fixing issue when reading orc metadata from DATE dtype
- #1338 Window Function support for LEAD and LAG statements 
- #1362 give useful message when file extension is not recognized
- #1361 Supporting first_value and last_value for Window Function


## Improvements
- #1293 Added optional acknowledgments to message sending
- #1236 Moving code from header files to implementation files
- #1257 Expose the reset max memory usage C++ API to python
- #1256 Improve Logical project documentation
- #1262 Stop depending on gtest for runtime
- #1261 Improve storage plugin output messages
- #1153 Enable warnings and fixes
- #1267 Added retrys to comms, fixed deadlocks in executor and order by. Improved logging and error management. Caches have names. Improved Joins
- #1239 Reducing Memory pressure by moving shuffle data to cpu before transmission
- #1278 Fix race conditions with UCX
- #1279 Added cuml to powerpc build scripts
- #1286 Fixes to initialization and adding unique ids to comms
- #1255 Kernels are resilient to out of memory errors now and can retry tasks that fail this way
- #1311 Add queries logger to physical plan
- #1308 Improve the engine loggers
- #1314 Added unit tests to verify that OOM error handling works well
- #1320 Revamping cache logger
- #1323 Made progress bar update continuously and stay after query is done 
- #1336 Improvements for the cache API
- #1483 Improve dependencies script

## Bug Fixes
- #1249 Fix compilation with cuda 11
- #1253 Fixed distribution so that its evenly distributes based of rowgroups
- #1204 Reenable json parser
- #1241 Fixed cython exception handling
- #1243 Fixed wrong CHAR regex replacing
- #1275 Fixed issue in StringUtil::findAndReplaceAll when there are several matches
- #1277 Support FileSystems (GS, S3) when extension of the files are not provided
- #1300 Fixed issue when creating tables from a local dir relative path
- #1312 Fix progress bar for jupyterlab
- #1318 Disabled require acknowledge 

# BlazingSQL 0.17.0 (December 10, 2020)

## New Features
- #1105 Implement to_date/to_timestamp functions
- #1077 Allow to create tables from compressed files
- #1126 Add DAYOFWEEK function
- #981 Added powerPC building script and instructions
- #912 Added UCX support to how the engine runs
- #1125 Implement new TCP and UCX comms layer, exposed graph to python
- #1122 Add ConfigOptionsTest, a test with different config_options values
- #1110 Adding local logging directory to BlazingContext
- #1148 Add e2e test for DAYOFWEEK
- #1130 Infer hive folder partition
- #1188 Implement upper/lower operators
- #1193 Implement string REPLACE
- #1218 Added smiles test set
- #1201 Implement string TRIM
- #1216 Add unit test for DAYOFWEEK
- #1205 Implement string REVERSE
- #1220 Implement string LEFT and RIGHT 
- #1223 Add support for UNION statement
- #1250 updated README.md and CHANGELOG and others preparing for 0.17 release


## Improvements
- #878 Adding calcite rule for window functions. (Window functions not supported yet)
- #1081 Add validation for the kwargs when bc API is called
- #1082 Validate s3 bucket
- #1093 Logs configurable to have max size and be rotated
- #1091 Improves the error message problem when validating any GCP bucket
- #1102 Add option to read csv files in chunks
- #1090 Add tests for Uri Data provider for local uri
- #1119 Add tests for transform json tree and get json plan
- #1117 Add error logging in DataSourceSequence
- #1111 output compile json for cppcheck
- #1132 Refactoring new comms
- #1078 Bump junit from 4.12 to 4.13.1 in /algebra
- #1144 update with changes from main
- #1156 Added scheduler file support for e2e testing framework
- #1158 Deprecated bc.partition
- #1154 Recompute the avg_bytes_per_row value
- #1155 Removing comms subproject and cleaning some related code
- #1170 Improve gpuCI scripts
- #1194 Powerpc building scripts
- #1186 Removing cuda labels to install due cudatoolkit version
- #1187 Enable MySQL-specific SQL operators in addition to Standard and Oracle
- #1206 Improved contribution documentation
- #1224 Added cudaSetDevice to thread initialization so that the cuda context is available to UCX
- #1229 Change hardcoded version from setup.py
- #1231 Adding docker support for gpuCI scripts
- #1248 Jenkins and Docker scripts were improved for building


## Bug Fixes
- #1064 Fixed issue when loading parquet files with local_files=True
- #1086 Showing an appropriate error to indicate that we don't support opening directories with wildcards
- #1088 Fixed issue caused by cudf changing from one .so file to multiple
- #1094 Fixed logging directory setup
- #1100 Showing an appropriate error for invalid or unsupported expressions on the logical plan
- #1115 Fixed changes to RMM api using cuda_stream_view instead of cudaStream_t now
- #1120 Fix missing valid kwargs in create_table
- #1118 Fixed issue with config_options and adding local_files to valid params
- #1133 Fixed adressing issue in float columns when parsing parquet metadata
- #1163 added empty line to trigger build
- #1108 Remove temp files when an error occurs
- #1165 E2e tests, distributed mode, again tcp
- #1171 Don't log timeout in output/input caches
- #1168 Fix SSL errors for conda
- #1164 MergeAggr when single node has multiple batches
- #1191 Fix graph thread pool hang when exception is thrown
- #1181 Remove unnecesary prints (cluster and logging info)
- #1185 Create table in distributed mode crash with a InferFolderPartitionMetadata Error
- #1179 Fix ignore headers when multiple CSV files was provided
- #1199 Fix non thread-safe access to map containing tag to message_metadata for ucx
- #1196 Fix column_names (table) always as list of string
- #1203 Changed code back so that parquet is not read a single rowgroup at a time
- #1207 Calcite uses literal as int32 if not explicit CAST was provided
- #1212 Fixed issue when building the thirdpart, cmake version set to 3.18.4
- #1225 Fixed issue due to change in gather API 
- #1254 Fixing support of nightly and stable on localhost
- #1258 Fixing gtest version issue


# BlazingSQL 0.16.0 (October 22, 2020)

## Improvements
- #997 Add capacity to set the transport memory
- #1040 Update conda recipe, remove cxx11 abi from cmake
- #977 Just one initialize() function at beginning and add logs related to allocation stuff
- #1046 Make possible to read the system environment variables to set up BlazingContext
- #998 Update TPCH queries, become implicit joins into implicit joins to avoid random values.
- #1055 Removing cudf source code dependency as some cudf utilities headers were exposed
- #1065 Remove thrift from build prodcess as its no longer used
- #1067 Upload conda packages to both rapidsai and blazingsql conda channels


## Bug Fixes
- #918 Activate validation for GPU_CI tests results.
- #975 Fixed issue due to cudf orc api change
- #1017 Fixed issue parsing fixed with string literals
- #1019 Fix hive string col
- #1021 removed an rmm include
- #1020 Fixed build issues with latest rmm 0.16 and columnBasisTest due to deprecated drop_column() function
- #1029 Fix metadata mistmatch due to parsedMetadata
- #1016 Removed workaround for parquet read schema
- #1022 Fix pinned buffer pool
- #1028 Match dtypes after create_table with multiple files
- #1030 Avoid read _metadata files
- #1039 Fixed issues with parsers, in particular ORC parser was misbehaving
- #1038 Fixed issue with logging dirs in distributed envs
- #1048 Pinned google cloud version to 1.16
- #1052 Partial revert of some changes on parquet rowgroups flow with local_files=True
- #1054 Can set manually BLAZING_CHACHE_DIRECTORY
- #1053 Fixed issue when loading paths with wildcards
- #1057 Fixed issue with concat all in concatenating cache
- #1007 Fix arrow and spdlog compilation issues
- #1068 Just adds a docs important links and avoid the message about filesystem authority not found
- #1073 Fixed parseSchemaPython can throw exceptions
- #1074 Remove lock inside grow() method from PinnedBufferProvider
- #1071 Fix crash when loading an empty folder
- #1085 Fixed intra-query memory leak in joins. Fixed by clearing array caches after PartwiseJoin is done
- #1096 Backport from branch-0.17 with these PRs: #1094, #1086, #1093 and #1091
- #1099 Fixed issue with config_options


# BlazingSQL 0.15.0 (August 31, 2020)

## New Features
- #835 Added a memory monitor for better memory management and added pull ordered from cache
- #889 Added Sphinx based code architecture documentation
- #968 Support PowerPC architecture

## Improvements
- #777 Update Calcite to the most recent version 1.23
- #786 Added check for concat String overflow
- #815 Implemented Unordered pull from cache to help performance
- #822 remove "from_cudf" code and cudf test utilities from engine code
- #824 Added a test on Calcite to compare the logical plans when the ruleset is updated
- #802 Support for timestampadd and constant expressions evaluation by Calcite
- #849 Added check for CUDF_HOME to allow build to use an existing prebuilt cudf source tree
- #829 Python/Cython check code style
- #826 Support cross join
- #866 Added nogil statements for pure C functions in Cython
- #784 Updated set of TPCH queries on the E2E tests
- #877 round robing dask workers on single gpu queries
- #880 reraising query errors in context.py
- #883 add rand() and running unary operations on literals
- #894 added exhale to generate doxygen for sphinx docs
- #887 concatenating cache improvement and replacing PartwiseJoin::load_set with a concatenating cache
- #885 Added initial set of unit tests for `WaitingQueue` and nullptr checks around spdlog calls
- #904 Added doxygen comments to CacheMachine.h
- #901 Added more documentation about memory management
- #910 updated readme
- #915 Adding max kernel num threads pool
- #921 Make AWS and GCS optional
- #925 Replace random_generator with cudf::sample
- #900 Added doxygen comments to some kernels and the batch processing
- #936 Adding extern C for include files
- #941 Logging level (flush_on) can be configurable
- #947 Use default client and network interface from Dask
- #945 Added new separate thresh for concat cache
- #939 Add unit test for Project kernel
- #949 Implemented using threadpool for outgoing messages
- #961 Add list_tables() and describe_table() functions
- #967 Add bc.get_free_memory() function

## Bug Fixes
- #774 fixed build issues with latest cudf 0.15 including updating from_cudf
- #781 Fixed issue with Hive partitions when doing SELECT *
- #754 Normalize columns before distribution in JoinPartitionKernel
- #782 fixed issue with hive partitions base folder
- #791 Fixes issues due to changes in rmm and fixes allocator issues
- #770 Fix interops operators output types
- #798 Fix when the algebra plan was provided using one-line as logical plan
- #799 Fix uri values computacion in runQueryCaller
- #792 Remove orc temp files when cached on Disk
- #814 Fix when checking only Limit and Scan Kernels
- #816 Loading one file at a time (LimitKernel and ScanKernel)
- #832 updated calcite test reference
- #834 Fixed small issue with hive and cudf_type_int_to_np_types
- #839 Fixes literal cast
- #838 Fixed issue with start and length of substring being different types
- #823 Fixed issue on logical plans when there is an EXISTS clause
- #845 Fixed issue with casting string to string
- #850 Fixed issue with getTableScanInfoCaller
- #851 Fix row_groups issue in ParquetParser.cpp
- #847 Fixed issue with some constant expressions not evaluated by calcite
- #875 Recovered some old unit tests and deleted obsolete unit tests
- #879 Fixed issue with log directory creation in a distributed environment
- #890 Fixed issue where we were including testing hpp in our code
- #891 Fixed issue caused by replacing join load_set with concatenating cache
- #902 Fixed optimization regression on the select count(*) case
- #909 Fixed issue caused by using now arrow_io_source
- #913 Fixed issues caused by cudf adding DECIMAL data type
- #916 Fix e2e string comparison
- #927 Fixed random segfault issue in parser
- #929 Update the GPUManager functions
- #942 Fix column names on sample function
- #950 Introducing config param for max orderby samples and fixing oversampling
- #952 Dummy PR
- #957 Fixed issues caused by changes to timespamp in cudf
- #962 Use new rmm API instead of get_device_resource() and set_device_resource() functions
- #965 Handle exceptions from pool_threads
- #963 Set log_level when using LOGGING_LEVEL param
- #973 Fix how we check the existence of the JAVA_HOME environment variable

# BlazingSQL 0.14.0 (June 9, 2020)

- #391 Added the ability to run count distinct queries in a distruted fashion
- #392 Remove the unnecessary messages on distributed mode
- #560 Fixed bug where parsing errors would lead to crash
- #565 made us have same behaviour as cudf for reading csv
- #612 Print product version: print(blazingsql.__version__) # shows the git hash
- #638 Refactores and fixes SortAndSample kernels
- #631 Implemented ability to send config_options to bc.sql function
- #621 Clean dead code
- #602 Implements cache flow control feature
- #625 Implement CAST to TINYINT and SMALLINT
- #632 Implement CHAR_LENGTH function
- #635 Handle behavior when the optimized plan contains a LogicalValues
- #653 Handle exceptions on python side
- #661 added hive support to parse_batch
- #662 updated from_cudf code and fixed other issue due to new cudf::list_view
- #674 Allow to define and use a specific AWS S3 region
- #677 added guava to pom.xml
- #679 Support modern compilers (>= g++-7.x)
- #649 Adding event logging
- #660 Changed how we handle the partitions of a dask.cudf.DataFrame
- #697 Update expression parser
- #659 Improve reading for: SELECT * FROM table LIMIT N
- #700 Support null column in projection
- #711 Migrate end to end tests into blazingsql repo
- #718 Changed all condition variable waits to wait_for
- #712 fixed how we handle empty tables for estimate for small table join
- #724 Removed unused BlazingThread creations
- #725 Added nullptr check to num_rows()
- #729 Fixed issue with num_rows() and wait_for
- #728 Add replace_calcite_regex function to the join condition
- #721 Handling multi-partition output
- #750 Each table scan now has its own data loader
- #740 Normalizing types for UNION ALL
- #744 Fix unit tests
- #743 Workaround for interops 64 index plan limitation
- #763 Implemented ability to set the folder for all log files
- #757 Ensure GPU portability (so we can run on any cloud instance with GPU)
- #753 Fix for host memory threshold parameter with Dask envs
- #801 Fix build with new cudf 0.15 and arrow 0.17.1
- #809 Fix conda build issues
- #828 Fix gpuci issues and improve tooling to debug gpuci related issues
- #867 Fix boost dependencie issues
- #785 Add script for Manual Testing Artifacts.
- #931 Add script for error messages validation.
- #932 Import pydrill and pyspark only when its generator or full mode.
- #1031 adding notebooks into BlazingSQL Tests
- #1486 Define generic templates for E2E Testing framework.
- #1542 Cleaning code on E2E Test framework.
