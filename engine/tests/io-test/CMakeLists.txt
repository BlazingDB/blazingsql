set(parse_csv-test_SRCS
  parse_csv.cu
)
  
set(parse_parquet-test_SRCS
    parse_parquet.cu
)
 
configure_test(parse_csv-test "${parse_csv-test_SRCS}")

#TODO William
#configure_test(parse_parquet-test "${parse_parquet-test_SRCS}")
