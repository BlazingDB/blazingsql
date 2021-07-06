import cudf


expected = cudf.DataFrame({
  'o_orderkey': [2.0, 3.0, 4.0, 5.0, 6.0]
}).to_pandas()
