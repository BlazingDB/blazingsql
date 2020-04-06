> A lightweight, GPU accelerated, SQL engine built on the [RAPIDS.ai](https://rapids.ai) ecosystem.

<a href='https://colab.research.google.com/drive/1r7S15Ie33yRw8cmET7_bjCpvjJiDOdub'> <p align="center"><img src="https://github.com/BlazingDB/blazingsql/blob/roaramburu-readme-update/img/bsql_rapids.PNG"/></p></a>

[Getting Started](https://github.com/BlazingDB/blazingsql#getting-started) | [Documentation](https://docs.blazingdb.com) | [Examples](https://github.com/BlazingDB/blazingsql#examples) | [Contributing](https://github.com/BlazingDB/blazingsql#contributing) | [License](https://github.com/BlazingDB/blazingsql/blob/develop/LICENSE) | [Blog](https://blog.blazingdb.com)

BlazingSQL is a GPU accelerated SQL engine built on top of the RAPIDS ecosystem. RAPIDS is based on the [Apache Arrow](http://arrow.apache.org) columnar memory format, and [cuDF](https://github.com/rapidsai/cudf) is a GPU DataFrame library for loading, joining, aggregating, filtering, and otherwise manipulating data.

BlazingSQL is a SQL interface for cuDF, with various features to support large scale data science workflows and enterprise datasets.
* **Query Data Stored Externally** - a single line of code can register remote storage solutions, such as Amazon S3.
* **Simple SQL** - incredibly easy to use, run a SQL query and the results are GPU DataFrames (GDFs).
* **Interoperable** - GDFs are immediately accessible to any [RAPIDS](htts://github.com/rapidsai) library for data science workloads.

Check out our 5-min quick start notebook [![Google Colab Badge](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/drive/1r7S15Ie33yRw8cmET7_bjCpvjJiDOdub) using BlazingSQL.

# Getting Started

Please reference our [docs](https://docs.blazingdb.com/docs/blazingsql) to find out how to install BlazingSQL.

Querying a CSV file in Amazon S3 with BlazingSQL:

For example:
```python
from blazingsql import BlazingContext
bc = BlazingContext()

bc.s3('dir_name', bucket_name='bucket_name', access_key_id='access_key', secret_key='secret_key')

# Create Table from CSV
bc.create_table('taxi', '/dir_name/taxi.csv')

# Query
result_gdf = bc.sql('SELECT count(*) FROM taxi GROUP BY year(key)')

#Print GDF
print(result_gdf)
```
## Examples

* Getting Started Guide - [Google Colab](https://colab.research.google.com/drive/1r7S15Ie33yRw8cmET7_bjCpvjJiDOdub#scrollTo=14GwxmLsTV_p)
* Netflow Demo - [Google Colab](https://colab.research.google.com/drive/1RYOYthqxUl922LYMAuNneKgmWB8YGTKB)
* Taxi cuML Linear Regression - [Google Colab](https://colab.research.google.com/drive/10il0C55uRhsgu2vqRVLqdB7Zp0gDt8Me)

## Documentation
You can find our full documentation at [the following site](https://docs.blazingdb.com/docs/)


## Quick Start

Too see all the ways you can get started with BlazingSQL checkout out our [Getting Started Page](https://blazingsql.com/#/getstarted)

# Install Using Conda
BlazingSQL can be installed with conda ([miniconda](https://conda.io/miniconda.html), or the full [Anaconda distribution](https://www.anaconda.com/download)) from the [blazingsql](https://anaconda.org/blazingsql/) channel:

Note: BlazingSQL is supported only on Linux, and with Python version 3.6 and 3.7.

## Stable Version 
*For CUDA 9.2 and Python 3.7:*
```bash
conda install -c blazingsql/label/cuda9.2 -c blazingsql -c rapidsai -c nvidia -c conda-forge -c defaults blazingsql python=3.7 cudatoolkit=9.2
```

*For CUDA 10.0 and Python 3.7:*
```bash
conda install -c blazingsql/label/cuda10.0 -c blazingsql -c rapidsai -c nvidia -c conda-forge -c defaults blazingsql python=3.7 cudatoolkit=10.0
```

## Nightly Version
*For CUDA 9.2 and Python 3.7:*
```bash
conda install -c blazingsql-nightly/label/cuda9.2 -c blazingsql-nightly -c rapidsai-nightly -c conda-forge -c defaults blazingsql python=3.7
```

*For CUDA 10.0 and Python 3.7:*
```bash
conda install -c blazingsql-nightly/label/cuda10.0 -c blazingsql-nightly -c rapidsai-nightly -c conda-forge -c defaults blazingsql python=3.7
```

# Build/Install from Source (Conda Environment)
This is the recommended way of building all of the BlazingSQL components and dependencies from source. It ensures that all the dependencies are available to the build process.

## Stable Version

### Install build dependencies
*For CUDA 9.2 and Python 3.7:*
```bash
conda create -n bsql python=3.7
conda activate bsql
conda install --yes -c conda-forge openjdk=8.0 maven cmake gtest gmock rapidjson cppzmq cython=0.29 jpype1 netifaces pyhive
conda install --yes -c conda-forge -c blazingsql bsql-toolchain
conda install --yes -c rapidsai -c nvidia -c conda-forge -c defaults cudf=0.12 dask-cudf=0.12 dask-cuda=0.12 cudatoolkit=9.2
```

*For CUDA 10.0 and Python 3.7:*
```bash
conda create -n bsql python=3.7
conda activate bsql
conda install --yes -c conda-forge openjdk=8.0 maven cmake gtest gmock rapidjson cppzmq cython=0.29 jpype1 netifaces pyhive
conda install --yes -c conda-forge -c blazingsql bsql-toolchain
conda install --yes -c rapidsai -c nvidia -c conda-forge -c defaults cudf=0.12 dask-cudf=0.12 dask-cuda=0.12 cudatoolkit=10.0
```

### Build
The build process will checkout the BlazingSQL repository and will build and install into the conda environment.

```bash
cd $CONDA_PREFIX
git clone https://github.com/BlazingDB/blazingsql.git
cd blazingsql
git checkout master
export CUDACXX=/usr/local/cuda/bin/nvcc
./build.sh
```
NOTE: You can do `./build.sh -h` to see more build options.

$CONDA_PREFIX now has a folder for the blazingsql repository.

## Nightly Version

### Install build dependencies
*For CUDA 9.2:*
```bash
conda create -n bsql python=3.7
conda activate bsql
conda install --yes -c conda-forge openjdk=8.0 maven cmake gtest gmock rapidjson cppzmq cython=0.29 jpype1 netifaces pyhive
conda install --yes -c conda-forge -c blazingsql-nightly bsql-toolchain
conda install --yes -c rapidsai-nightly -c nvidia -c conda-forge -c defaults libcudf=0.13 cudf=0.13 dask-cudf=0.13 dask-cuda=0.13 cudatoolkit=9.2
```

*For CUDA 10.0:*
```bash
conda create -n bsql python=3.7
conda activate bsql
conda install --yes -c conda-forge openjdk=8.0 maven cmake gtest gmock rapidjson cppzmq cython=0.29 jpype1 netifaces pyhive
conda install --yes -c conda-forge -c blazingsql-nightly bsql-toolchain
conda install --yes -c rapidsai-nightly -c nvidia -c conda-forge -c defaults libcudf=0.13 cudf=0.13 dask-cudf=0.13 dask-cuda=0.13 cudatoolkit=10.0
```

### Build
The build process will checkout the BlazingSQL repository and will build and install into the conda environment.

```bash
cd $CONDA_PREFIX
git clone https://github.com/BlazingDB/blazingsql.git
cd blazingsql
export CUDACXX=/usr/local/cuda/bin/nvcc
./build.sh
```
NOTE: You can do `./build.sh -h` to see more build options.

$CONDA_PREFIX now has a folder for the blazingsql repository.

# Community
## Contributing
Have questions or feedback? Post a [new github issue](https://github.com/blazingdb/blazingsql/issues/new/choose).

Please see our [guide for contributing to BlazingSQL](CONTRIBUTING.md).

## Contact
Feel free to join our Slack chat room: [RAPIDS Slack Channel](https://join.slack.com/t/rapids-goai/shared_invite/enQtMjE0Njg5NDQ1MDQxLTJiN2FkNTFkYmQ2YjY1OGI4NTc5Y2NlODQ3ZDdiODEwYmRiNTFhMzNlNTU5ZWJhZjA3NTg4NDZkMThkNTkxMGQ)

You may also email us at [info@blazingsql.com](info@blazingsql.com) or find out more details on the [BlazingSQL site](https://blazingsql.com)

## License
[Apache License 2.0](https://github.com/BlazingDB/blazingsql/blob/develop/LICENSE)

## RAPIDS AI - Open GPU Data Science

The RAPIDS suite of open source software libraries aim to enable execution of end-to-end data science and analytics pipelines entirely on GPUs. It relies on NVIDIA® CUDA® primitives for low-level compute optimization, but exposing that GPU parallelism and high-bandwidth memory speed through user-friendly Python interfaces.

## Apache Arrow on GPU

The GPU version of [Apache Arrow](https://arrow.apache.org/) is a common API that enables efficient interchange of tabular data between processes running on the GPU. End-to-end computation on the GPU avoids unnecessary copying and converting of data off the GPU, reducing compute time and cost for high-performance analytics common in artificial intelligence workloads. As the name implies, cuDF uses the Apache Arrow columnar data format on the GPU. Currently, a subset of the features in Apache Arrow are supported.
