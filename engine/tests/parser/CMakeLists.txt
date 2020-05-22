set(parser_sources
    expression_tree_test.cpp
)
# TODO fix this tests percy william
#configure_test(parser_test "${parser_sources}")

set(split_inequality_join_sources
    split_inequality_join_test.cpp
)
configure_test(split_inequality_join_test "${split_inequality_join_sources}")