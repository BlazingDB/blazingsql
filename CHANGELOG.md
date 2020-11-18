# BlazingSQL 0.17.0 (Date TBS)


## New Features
- #1105 Implement to_date/to_timestamp functions
- #1077 Allow to create tables from compressed files
- #1126 Add DAYOFWEEK function
- #981 Added powerPC building script and instructions
- #912 Added UCX support to how the engine runs
- #1125 Implement new TCP and UCX comms layer, exposed graph to python
- #1110 Adding local logging directory to BlazingContext
- #1148 Add e2e test for DAYOFWEEK
- #1130 Infer hive folder partition


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
- #1168 Fix SSL errors for conda 
- #1165 E2e tests, distributed mode, again tcp


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
