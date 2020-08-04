# BlazingSQL 0.15.0 (Date TBS)

## New Features
- #835 Added a memory monitor for better memory management and added pull ordered from cache
- #889 Added Sphinx based code architecture documentation

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
- #904 Added doxygen comments to CacheMachine.h
- #901 Added more documentation about memory management 
- #910 updated readme 
- #912 Added UCX support to how the engine runs

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
