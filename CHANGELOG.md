#####################################################################
# Use this file to document any changes made during a PR. Every PR  #
# should have an entry.                                             #
#####################################################################
#391 Added the ability to run count distinct queries in a distruted fashion
#392 Remove the unnecessary messages on distributed mode
#560 Fixed bug where parsing errors would lead to crash
#565 made us have same behaviour as cudf for reading csv
#612 Print product version: print(blazingsql.__version__) # shows the git hash
#626 Avoid the engine crashes when a LogicalValues is found out on the optimized plan