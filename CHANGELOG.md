#####################################################################
# Use this file to document any changes made during a PR. Every PR  #
# should have an entry.                                             #
#####################################################################
#391 Added the ability to run count distinct queries in a distruted fashion
#392 Remove the unnecessary messages on distributed mode
#560 Fixed bug where parsing errors would lead to crash
#565 made us have same behaviour as cudf for reading csv
#612 Print product version: print(blazingsql.__version__) # shows the git hash
#631 Implemented ability to send config_options to bc.sql function
#621 Clean dead code
#602 Implements cache flow control feature
#625 Implement CAST to TINYINT and SMALLINT
#635 Handle behavior when the optimized plan contains a LogicalValues
#661 added hive support to parse_batch
#662 updated from_cudf code and fixed other issue due to new cudf::list_view
