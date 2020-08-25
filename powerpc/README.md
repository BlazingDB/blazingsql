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
