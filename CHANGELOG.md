#####################################################################
# Use this file to document any changes made during a PR. Every PR  #
# should have an entry.                                             #
#####################################################################
#391 Added the ability to run count distinct queries in a distruted fashion
#392 Remove the unnecessary messages on distributed mode
#560 Fixed bug where parsing errors would lead to crash
#565 made us have same behaviour as cudf for reading csv
#612 Print product version: print(blazingsql.__version__) # shows the git hash
#638 Refactores and fixes SortAndSample kernels
#631 Implemented ability to send config_options to bc.sql function
#621 Clean dead code
#602 Implements cache flow control feature
#625 Implement CAST to TINYINT and SMALLINT
#632 Implement CHAR_LENGTH function
#635 Handle behavior when the optimized plan contains a LogicalValues
#653 Handle exceptions on python side
#661 added hive support to parse_batch
#662 updated from_cudf code and fixed other issue due to new cudf::list_view
#674 Allow to define and use a specific AWS S3 region
#677 added guava to pom.xml
#679 Support modern compilers (>= g++-7.x)
#649 Adding event logging
#660 Changed how we handle the partitions of a dask.cudf.DataFrame
#697 Update expression parser
#659 Improve reading for: SELECT * FROM table LIMIT N
#700 Support null column in projection
#711 Migrate end to end tests into blazingsql repo
#718 Changed all condition variable waits to wait_for
#712 fixed how we handle empty tables for estimate for small table join
#724 Removed unused BlazingThread creations
#725 Added nullptr check to num_rows()
#729 Fixed issue with num_rows() and wait_for
#728 Add replace_calcite_regex function to the join condition
#721 Handling multi-partition output
#740 Normalizing types for UNION ALL
