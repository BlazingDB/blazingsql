## Readme

### Docker
Building docker image for build:
```
cd blazingsql/powerpc
./docker-build.sh
```

Run a container
```
cd blazingsql/powerpc
docker run -ti --rm blazingdb/build:powerpc bash
```

Run a container with volume
```
cd blazingsql/powerpc
docker run -ti -v $PWD:/app --rm blazingdb/build:powerpc bash
```

Run a container with gpu
```
cd blazingsql/powerpc
docker run -ti -v $PWD:/app --gpus=all --rm blazingdb/build:powerpc bash
```

Run a container with gpu and same user
```
cd blazingsql/powerpc
docker run -u $(id -u):$(id -g) -ti -v /etc/passwd:/etc/passwd -v $PWD:/app --gpus=all --rm blazingdb/build:powerpc bash
```

Execute a command as root:
```
docker exec -u 0:0 -ti <container_id> bash
```

The docker has a pip env in /opt/blazingsql-powerpc-prefix with all the requirements.txt installed

### Python Virtualenv

#### Using the docker pyenv

Activate:
```
source /opt/blazingsql-powerpc-prefix/bin/activate
pip list
```

#### Create new pyenvs

Create:
```
python3 -m venv demo
```

Activate:
```
source demo/bin/activate
pip list
```

Install Python dependencies:
```
pip install -r requirements.txt
```

Deactivate:
```
deactivate
```

Dockerfile
nvidia-docker doesn't have powerpc support so this image is only for debugging

#### Run python scripts
For now we need to run python as:
```shell
JAVA_HOME=/usr/lib/jvm/jre CONDA_PREFIX=/opt/blazingsql-powerpc-prefix/ LD_LIBRARY_PATH=/opt/blazingsql-powerpc-prefix/lib:/usr/local/lib64/:/usr/local/lib python
```

### Issues
#### CUDF issues

- CUDF seems to have some issues for binary operators.
Since cudf use JIT for the implementation it may be something related to unsupported JIT/cudaRTC/jitify for powepc

libcudf c++ unit test results for commut hash 4b6b7c0fdb14aec638173201d04078a060c0eab4
```shell
cd cudf/cpp/build
ctest
Test project /opt/blazingsql-powerpc-prefix/build/cudf/cpp/build
      Start  1: COLUMN_TEST
 1/54 Test  #1: COLUMN_TEST ......................   Passed    3.62 sec
      Start  2: SCALAR_TEST
 2/54 Test  #2: SCALAR_TEST ......................   Passed    3.56 sec
      Start  3: TIMESTAMPS_TEST
 3/54 Test  #3: TIMESTAMPS_TEST ..................   Passed    3.87 sec
      Start  4: ERROR_TEST
 4/54 Test  #4: ERROR_TEST .......................   Passed    3.49 sec
      Start  5: GROUPBY_TEST
 5/54 Test  #5: GROUPBY_TEST .....................***Failed   10.40 sec
      Start  6: JOIN_TEST
 6/54 Test  #6: JOIN_TEST ........................   Passed    4.09 sec
      Start  7: IS_SORTED_TEST
 7/54 Test  #7: IS_SORTED_TEST ...................   Passed    3.54 sec
      Start  8: DATETIME_OPS_TEST
 8/54 Test  #8: DATETIME_OPS_TEST ................   Passed    3.73 sec
      Start  9: HASHING_TEST
 9/54 Test  #9: HASHING_TEST .....................   Passed    3.92 sec
      Start 10: PARTITIONING_TEST
10/54 Test #10: PARTITIONING_TEST ................   Passed   15.85 sec
      Start 11: HASH_MAP_TEST
11/54 Test #11: HASH_MAP_TEST ....................   Passed    3.63 sec
      Start 12: QUANTILES_TEST
12/54 Test #12: QUANTILES_TEST ...................   Passed    4.68 sec
      Start 13: REDUCTION_TEST
13/54 Test #13: REDUCTION_TEST ...................   Passed    4.15 sec
      Start 14: REPLACE_TEST
14/54 Test #14: REPLACE_TEST .....................   Passed    4.42 sec
      Start 15: REPLACE_NULLS_TEST
15/54 Test #15: REPLACE_NULLS_TEST ...............   Passed    3.94 sec
      Start 16: REPLACE_NANS_TEST
16/54 Test #16: REPLACE_NANS_TEST ................   Passed    3.62 sec
      Start 17: NORMALIZE_REPLACE_TEST
17/54 Test #17: NORMALIZE_REPLACE_TEST ...........   Passed    3.60 sec
      Start 18: CLAMP_TEST
18/54 Test #18: CLAMP_TEST .......................   Passed    4.00 sec
      Start 19: FIXED_POINT_TEST
19/54 Test #19: FIXED_POINT_TEST .................   Passed    3.45 sec
      Start 20: UNARY_TEST
20/54 Test #20: UNARY_TEST .......................   Passed    4.34 sec
      Start 21: BINARY_TEST
21/54 Test #21: BINARY_TEST ......................***Failed    3.71 sec
      Start 22: TRANSFORM_TEST
22/54 Test #22: TRANSFORM_TEST ...................***Failed    3.89 sec
      Start 23: INTEROP_TEST
23/54 Test #23: INTEROP_TEST .....................   Passed    3.76 sec
      Start 24: JITCACHE_TEST
24/54 Test #24: JITCACHE_TEST ....................***Exception: SegFault  3.76 sec
      Start 25: JITCACHE_MULTIPROC_TEST
25/54 Test #25: JITCACHE_MULTIPROC_TEST ..........***Exception: SegFault  0.06 sec
      Start 26: DECOMPRESSION_TEST
26/54 Test #26: DECOMPRESSION_TEST ...............   Passed    3.49 sec
      Start 27: CSV_TEST
27/54 Test #27: CSV_TEST .........................   Passed    3.67 sec
      Start 28: ORC_TEST
28/54 Test #28: ORC_TEST .........................   Passed    5.49 sec
      Start 29: PARQUET_TEST
29/54 Test #29: PARQUET_TEST .....................   Passed   17.77 sec
      Start 30: JSON_TEST
30/54 Test #30: JSON_TEST ........................   Passed    3.62 sec
      Start 31: SORT_TEST
31/54 Test #31: SORT_TEST ........................   Passed    8.45 sec
      Start 32: COPYING_TEST
32/54 Test #32: COPYING_TEST .....................   Passed    8.59 sec
      Start 33: UTILITIES_TEST
33/54 Test #33: UTILITIES_TEST ...................   Passed    5.78 sec
      Start 34: ITERATOR_TEST
34/54 Test #34: ITERATOR_TEST ....................   Passed    4.26 sec
      Start 35: DEVICE_ATOMICS_TEST
35/54 Test #35: DEVICE_ATOMICS_TEST ..............   Passed    4.57 sec
      Start 36: TRANSPOSE_TEST
36/54 Test #36: TRANSPOSE_TEST ...................   Passed  133.61 sec
      Start 37: TABLE_TEST
37/54 Test #37: TABLE_TEST .......................   Passed    3.73 sec
      Start 38: MERGE_TEST
38/54 Test #38: MERGE_TEST .......................   Passed    4.42 sec
      Start 39: STREAM_COMPACTION_TEST
39/54 Test #39: STREAM_COMPACTION_TEST ...........   Passed    3.62 sec
      Start 40: ROLLING_TEST
40/54 Test #40: ROLLING_TEST .....................***Exception: SegFault  8.23 sec
      Start 41: GROUPED_ROLLING_TEST
41/54 Test #41: GROUPED_ROLLING_TEST .............***Exception: SegFault  3.73 sec
      Start 42: FILLING_TEST
42/54 Test #42: FILLING_TEST .....................   Passed    4.48 sec
      Start 43: SEARCH_TEST
43/54 Test #43: SEARCH_TEST ......................   Passed    3.57 sec
      Start 44: RESHAPE_TEST
44/54 Test #44: RESHAPE_TEST .....................   Passed    3.80 sec
      Start 45: TRAITS_TEST
45/54 Test #45: TRAITS_TEST ......................   Passed    3.35 sec
      Start 46: FACTORIES_TEST
46/54 Test #46: FACTORIES_TEST ...................   Passed    3.46 sec
      Start 47: DISPATCHER_TEST
47/54 Test #47: DISPATCHER_TEST ..................   Passed    3.52 sec
      Start 48: STRINGS_TEST
48/54 Test #48: STRINGS_TEST .....................   Passed    4.28 sec
      Start 49: STRUCTS_TEST
49/54 Test #49: STRUCTS_TEST .....................   Passed    4.30 sec
      Start 50: TEXT_TEST
50/54 Test #50: TEXT_TEST ........................   Passed    3.57 sec
      Start 51: BITMASK_TEST
51/54 Test #51: BITMASK_TEST .....................   Passed    4.91 sec
      Start 52: DICTIONARY_TEST
52/54 Test #52: DICTIONARY_TEST ..................   Passed    3.66 sec
      Start 53: ENCODE_TEST
53/54 Test #53: ENCODE_TEST ......................   Passed    3.68 sec
      Start 54: LISTS_TEST
54/54 Test #54: LISTS_TEST .......................   Passed    3.82 sec
87% tests passed, 7 tests failed out of 54
Total Test time (real) = 384.52 sec
The following tests FAILED:
	  5 - GROUPBY_TEST (Failed)
	 21 - BINARY_TEST (Failed) => JITIFY (unit tests failed) => CUB
	 22 - TRANSFORM_TEST (Failed)
	 24 - JITCACHE_TEST (SEGFAULT)
	 25 - JITCACHE_MULTIPROC_TEST (SEGFAULT)
	 40 - ROLLING_TEST (SEGFAULT)
	 41 - GROUPED_ROLLING_TEST (SEGFAULT)
Errors while running CTest
```

### BlazingSQL Issues
blazingsql commit hash: b91d78da1a47d3539c3e963fa49ac78478ab3116
cudf commit hash: 1a3b3f217be93a55b47af3a9d0da29f0fcb7c7e9
rmm commit hash: 178c2cd2933fa80b70e69863ba727d19f951a551

Issues summary:
| Test                          | #Rals | Issue IDs  | Query Id                        |
| ----------------------------- | ----- | ---------- | ------------------------------- |
| coalesceTest                  | 2     | Issue 2    |                                 |
| columnBasisTest               | 1,2   | Issue 1    |                                 |
| Count without group by        | 2     | Issue 1    |                                 |
| Date                          | 2     | Issue 1    |                                 |
| Timestamp                     | 2     | Issue 1    |                                 |
| Full outer join               | 2     | Issue 1    |                                 |
| Group by                      | 2     | Issue 1    |                                 |
| Group by without aggregations | 2     | Issue 1    |                                 |
| Inner join                    | 2     | Issue 1    |                                 |
| Cross join                    | 2     | Issue 1    |                                 |
| Left outer join               | 2     | Issue 1    |                                 |
| Non-EquiJoin Queries          | 2     | Issue 1    |                                 |
| Nested Queries                | 2     | Issue 1    |                                 |
| Order by                      | 2     | Issue 1    |                                 |
| Predicates With Nulls         | 2     | Issue 1    |                                 |
| Simple String                 | 2     | Issue 1    |                                 |
| Tables from Pandas            | 2     | Issue 1    |                                 |
| Unify Tables                  | 2     | Issue 1    |                                 |
| Union                         | 2     | Issue 1    |                                 |
| Limit                         | 2     | Issue 1    |                                 |
| Where clause                  | 2     | Issue 1    |                                 |
| Bindable Alias                | 2     | Issue 1    |                                 |
| Boolean                       | 2     | Issue 1    |                                 |
| Case                          | 2     | Issue 1    |                                 |

Issue types:
* Issue 1 (cudf issue)
```python
MemoryError: std::bad_alloc --> at File "cudf/_lib/binaryop.pyx" in cudf._lib.binaryop.binaryop
```

* Issue 2 (cudf issue
```python
MemoryError: std::bad_alloc --> at File "cudf/_lib/binaryop.pyx" in cudf._lib.binaryop.binaryop
distributed.utils - ERROR - CUDA error at: ../include/rmm/mr/device/per_device_resource.hpp:134: cudaErrorCudartUnloading driver shutting down
RuntimeError: CUDA error at: ../include/rmm/mr/device/per_device_resource.hpp:134: cudaErrorCudartUnloading driver shutting down
distributed.protocol.core - CRITICAL - Failed to deserialize
RuntimeError: CUDA error at: ../include/rmm/mr/device/per_device_resource.hpp:134: cudaErrorCudartUnloading driver shutting down
```

#### e2e single node

```shell
========================================================
DETAILED SUMMARY TESTS
========================================================
TestGroup                       InputType  Result 
Aggregations without group by   gdf        Success     7
                                orc        Success     6
                                parquet    Success     7
                                psv        Success     7
Bindable Alias                  gdf        Success    12
                                orc        Success    12
                                parquet    Success    12
                                psv        Success    12
Boolean                         psv        Success    10
Case                            gdf        Success    10
                                orc        Success    10
                                parquet    Success    10
                                psv        Success    10
Cast                            gdf        Success    13
                                orc        Success    12
                                parquet    Success    13
                                psv        Success    13
Coalesce                        parquet    Success    11
                                psv        Success    11
Common Table Expressions        gdf        Success     3
                                orc        Success     3
                                parquet    Success     3
                                psv        Success     3
Concat                          gdf        Success    10
                                orc        Success    10
                                parquet    Success    10
                                psv        Success    10
Count without group by          gdf        Success     6
                                parquet    Success     6
                                psv        Success     6
Cross join                      gdf        Success     4
                                orc        Success     4
                                parquet    Success     4
                                psv        Success     4
Date                            gdf        Success     5
                                orc        Success     5
                                parquet    Success     5
                                psv        Success     5
Dir                             gdf        Success    22
File System Local               orc        Success    21
                                parquet    Success    21
                                psv        Success    21
Full outer join                 gdf        Success     4
                                orc        Success     4
                                parquet    Success     4
                                psv        Success     4
Group by                        gdf        Success     6
                                orc        Success     5
                                parquet    Success     6
                                psv        Success     6
Group by without aggregations   gdf        Success     5
                                orc        Success     5
                                parquet    Success     5
                                psv        Success     5
Inner join                      gdf        Success    10
                                orc        Success    10
                                parquet    Success    10
                                psv        Success    10
Left outer join                 gdf        Success     4
                                orc        Success     4
                                parquet    Success     4
                                psv        Success     4
Like                            gdf        Success     3
Limit                           gdf        Success    10
                                orc        Success    10
                                parquet    Success    10
                                psv        Success    10
Literal                         orc        Success     5
Message Validation              gdf        Success     4
                                orc        Success     4
                                parquet    Success     4
                                psv        Success     4
Nested Queries                  gdf        Success     4
                                orc        Success     4
                                parquet    Success     4
                                psv        Success     4
Non-EquiJoin Queries            gdf        Success     8
                                orc        Success     8
                                parquet    Success     8
                                psv        Success     8
Order by                        gdf        Success     4
                                orc        Success     4
                                parquet    Success     4
                                psv        Success     4
Predicates With Nulls           gdf        Success     7
                                orc        Success     7
                                parquet    Success     7
                                psv        Success     7
Round                           gdf        Success     5
                                orc        Success     5
                                parquet    Success     5
                                psv        Success     5
Simple Distribution From Local  gdf        Success    52
                                orc        Success    52
                                parquet    Success    52
                                psv        Success    52
Simple String                   gdf        Success     9
                                orc        Success     9
                                parquet    Success     9
                                psv        Success     9
Substring                       gdf        Success    11
                                orc        Success    11
                                parquet    Success    11
                                psv        Success    11
TPCH Queries                    gdf        Success    17
                                orc        Success    17
                                parquet    Success    17
                                psv        Success    17
Tables from Pandas              gdf        Success    15
Timestamp                       orc        Success    12
Unary ops                       gdf        Success     6
                                orc        Success     6
                                parquet    Success     6
                                psv        Success     6
Unify Tables                    gdf        Success     8
                                orc        Success     8
                                parquet    Success     8
                                psv        Success     8
Union                           gdf        Success     7
                                orc        Success     7
                                parquet    Success     7
                                psv        Success     7
Where clause                    gdf        Success    11
                                orc        Success    11
                                parquet    Success    11
                                psv        Success    11
Wild Card                       gdf        Success    21
                                orc        Success    21
                                parquet    Success    21
                                psv        Success    21
Name: Result, dtype: int64
========================================================
FAILED TESTS
========================================================
Empty DataFrame
Columns: [index, TestId]
Index: []
**********************************************************
          *********************
TOTAL SUMMARY for test suite: 
PASSED: 1275/1275
FAILED: 0/1275
CRASH: 0/1275
TOTAL: 1275
MAX DELTA: 970.0
***********************************************************
              ********************
Aggregations without group by:   Start Mem: 707.625   End Mem: 1677.625   Diff: 970.0
Coalesce:   Start Mem: 1677.625   End Mem: 1563.625   Diff: -114.0
Common Table Expressions:   Start Mem: 1563.625   End Mem: 1637.625   Diff: 74.0
Count without group by:   Start Mem: 1637.625   End Mem: 1585.625   Diff: -52.0
Date:   Start Mem: 1585.625   End Mem: 1633.625   Diff: 48.0
Timestamp:   Start Mem: 1633.625   End Mem: 1635.625   Diff: 2.0
Full outer join:   Start Mem: 1635.625   End Mem: 1585.625   Diff: -50.0
Group by:   Start Mem: 1585.625   End Mem: 1555.625   Diff: -30.0
Group by without aggregations:   Start Mem: 1555.625   End Mem: 1665.625   Diff: 110.0
Inner join:   Start Mem: 1665.625   End Mem: 1569.625   Diff: -96.0
Cross join:   Start Mem: 1569.625   End Mem: 1637.625   Diff: 68.0
Left outer join:   Start Mem: 1637.625   End Mem: 1605.625   Diff: -32.0
Non-EquiJoin Queries:   Start Mem: 1605.625   End Mem: 1635.625   Diff: 30.0
Nested Queries:   Start Mem: 1635.625   End Mem: 1597.625   Diff: -38.0
Order by:   Start Mem: 1597.625   End Mem: 1595.625   Diff: -2.0
Predicates With Nulls:   Start Mem: 1595.625   End Mem: 1561.625   Diff: -34.0
Simple String:   Start Mem: 1561.625   End Mem: 1661.625   Diff: 100.0
Unary ops:   Start Mem: 1803.625   End Mem: 1773.625   Diff: -30.0
Unify Tables:   Start Mem: 1773.625   End Mem: 1683.625   Diff: -90.0
Union:   Start Mem: 1683.625   End Mem: 1691.625   Diff: 8.0
Limit:   Start Mem: 1691.625   End Mem: 1609.625   Diff: -82.0
Where clause:   Start Mem: 1609.625   End Mem: 1571.625   Diff: -38.0
Bindable Alias:   Start Mem: 1571.625   End Mem: 1627.625   Diff: 56.0
Boolean:   Start Mem: 1627.625   End Mem: 1617.625   Diff: -10.0
Case:   Start Mem: 1617.625   End Mem: 1657.625   Diff: 40.0
Cast:   Start Mem: 1657.625   End Mem: 1647.625   Diff: -10.0
Concat:   Start Mem: 1647.625   End Mem: 1595.625   Diff: -52.0
Literal:   Start Mem: 1595.625   End Mem: 1675.625   Diff: 80.0
Dir:   Start Mem: 1675.625   End Mem: 1779.625   Diff: 104.0
Like:   Start Mem: 1779.625   End Mem: 1819.625   Diff: 40.0
Substring:   Start Mem: 1655.625   End Mem: 1593.625   Diff: -62.0
Wild Card:   Start Mem: 1593.625   End Mem: 1581.625   Diff: -12.0
TPCH Queries:   Start Mem: 1581.625   End Mem: 1623.625   Diff: 42.0
Round:   Start Mem: 1623.625   End Mem: 1617.625   Diff: -6.0
File System Local:   Start Mem: 1617.625   End Mem: 1599.625   Diff: -18.0
Message Validation:   Start Mem: 1599.625   End Mem: 1617.625   Diff: 18.0

>>>> Total time for end to end tests: 5 minutes and 23 seconds
```

#### e2e multi node (2 rals)
```shell
```
