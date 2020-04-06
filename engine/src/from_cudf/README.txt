This folder (from_cudf) contains code straight from cudf repo with only small modifications to the includes so that it compiles. 

If you add anything more, please update all the code so that its all fromt he same commit hash and update the used commit hash here. 
If you add anything more, place any new files in a folder that matches its source location

This code was updated on 02/06/2020 with cudf commit hash  b451ce60ae6310d9cb7b5a2d7fabc8dae79f581d

Note that in table_utilities.cu I manually changed from
cudf::test::expect_columns_equal(lhs.column(i), rhs.column(i));
to:
cudf::test::expect_columns_equivalent(lhs.column(i), rhs.column(i));


