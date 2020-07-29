> A lightweight, GPU accelerated, SQL engine built on the [RAPIDS.ai](https://rapids.ai) ecosystem.

<a href='https://app.blazingsql.com/jupyter/user-redirect/lab/workspaces/auto-b/tree/Welcome_to_BlazingSQL_Notebooks/welcome.ipynb'><img src="https://github.com/BlazingDB/blazingsql/blob/roaramburu-readme-update/img/bsql_rapids.PNG"/></a>

[Getting Started](#getting-started) | [Documentation](https://docs.blazingdb.com) | [Examples](#examples) | [Contributing](#contributing) | [License](LICENSE) | [Blog](https://blog.blazingdb.com) | [Try Now](https://app.blazingsql.com/jupyter/user-redirect/lab/workspaces/auto-b/tree/Welcome_to_BlazingSQL_Notebooks/welcome.ipynb)

BlazingSQL is a GPU accelerated SQL engine built on top of the RAPIDS ecosystem. RAPIDS is based on the [Apache Arrow](http://arrow.apache.org) columnar memory format, and [cuDF](https://github.com/rapidsai/cudf) is a GPU DataFrame library for loading, joining, aggregating, filtering, and otherwise manipulating data.

BlazingSQL is a SQL interface for cuDF, with various features to support large scale data science workflows and enterprise datasets.
* **Query Data Stored Externally** - a single line of code can register remote storage solutions, such as Amazon S3.
* **Simple SQL** - incredibly easy to use, run a SQL query and the results are GPU DataFrames (GDFs).
* **Interoperable** - GDFs are immediately accessible to any [RAPIDS](htts://github.com/rapidsai) library for data science workloads.

Try our 5-min [Welcome Notebook](https://app.blazingsql.com/jupyter/user-redirect/lab/workspaces/auto-b/tree/Welcome_to_BlazingSQL_Notebooks/welcome.ipynb) to start using BlazingSQL and RAPIDS AI.

# Getting Started

Here's two copy + paste reproducable BlazingSQL snippets, keep scrolling to find [example Notebooks](#examples) below.

Create and query a table from a `cudf.DataFrame`:

```python
import cudf

df = cudf.DataFrame()

df['key'] = ['a', 'b', 'c', 'd', 'e']
df['val'] = [7.6, 2.9, 7.1, 1.6, 2.2]

from blazingsql import BlazingContext
bc = BlazingContext()

bc.create_table('game_1', df)

bc.sql('SELECT * FROM game_1 WHERE val > 4')
```

| | Key | Value |
| - | -:| ---:|
| 0 | a | 7.6 |
| 1 | b | 7.1 |

Create and query a table from a AWS S3 bucket:

```python
from blazingsql import BlazingContext
bc = BlazingContext()

bc.s3('blazingsql-colab', bucket_name='blazingsql-colab')

bc.create_table('taxi', 's3://blazingsql-colab/yellow_taxi/taxi_data.parquet')

bc.sql('SELECT passenger_count, trip_distance FROM taxi LIMIT 2')
```

| | passenger_count | fare_amount |
| - | -:| ---:|
| 0 | 1.0 | 1.1 |
| 1 | 1.0 | 0.7 |

## Examples

| Notebook Title | Description | Try Now |
| -------------- | ----------- | ------- |
| Welcome Notebook | An introduction to BlazingSQL Notebooks and the GPU Data Science Ecosystem. | <a href='https://app.blazingsql.com/jupyter/user-redirect/lab/workspaces/auto-b/tree/Welcome_to_BlazingSQL_Notebooks/welcome.ipynb'><img src="https://blazingsql.com/launch-notebooks.png" alt="Launch on BlazingSQL Notebooks" width="500"/></a> |
| The DataFrame | Learn how to use BlazingSQL and cuDF to create GPU DataFrames with SQL and Pandas-like APIs. | <a href='https://app.blazingsql.com/jupyter/user-redirect/lab/workspaces/auto-b/tree/Welcome_to_BlazingSQL_Notebooks/intro_notebooks/the_dataframe.ipynb'><img src="https://blazingsql.com/launch-notebooks.png" alt="Launch on BlazingSQL Notebooks" width="500"/></a> |
| Data Visualization | Plug in your favorite Python visualization packages, or use GPU accelerated visualization tools to render millions of rows in a flash. | <a href='https://app.blazingsql.com/jupyter/user-redirect/lab/workspaces/auto-b/tree/Welcome_to_BlazingSQL_Notebooks/intro_notebooks/data_visualization.ipynb'><img src="https://blazingsql.com/launch-notebooks.png" alt="Launch on BlazingSQL Notebooks" width="500"/></a> |
| Machine Learning | Learn about cuML, mirrored after the Scikit-Learn API, it offers GPU accelerated machine learning on GPU DataFrames. | <a href='https://app.blazingsql.com/jupyter/user-redirect/lab/workspaces/auto-b/tree/Welcome_to_BlazingSQL_Notebooks/intro_notebooks/machine_learning.ipynb'><img src="https://blazingsql.com/launch-notebooks.png" alt="Launch on BlazingSQL Notebooks" width="500"/></a> |

## Documentation
You can find our full documentation at [docs.blazingdb.com](https://docs.blazingdb.com/docs).

# Install Using Conda
BlazingSQL can be installed with conda ([miniconda](https://conda.io/miniconda.html), or the full [Anaconda distribution](https://www.anaconda.com/download)) from the [blazingsql](https://anaconda.org/blazingsql/) channel:

Note: BlazingSQL is supported only on Linux, and with Python version 3.6 and 3.7.

## Stable Version
```bash
conda install -c blazingsql/label/cuda$CUDA_VERSION -c blazingsql -c rapidsai -c nvidia -c conda-forge -c defaults blazingsql python=$PYTHON_VERSION
```
Where $CUDA_VERSION is 10.0, 10.1 or 10.2 and $PYTHON_VERSION is 3.6 or 3.7
*For example for CUDA 10.0 and Python 3.7:*
```bash
conda install -c blazingsql/label/cuda10.0 -c blazingsql -c rapidsai -c nvidia -c conda-forge -c defaults blazingsql python=3.7
``` 

## Nightly Version
```bash
conda install -c blazingsql-nightly/label/cuda$CUDA_VERSION -c blazingsql-nightly -c rapidsai-nightly -c nvidia -c conda-forge -c defaults blazingsql python=$PYTHON_VERSION
```
Where $CUDA_VERSION is 10.0, 10.1 or 10.2 and $PYTHON_VERSION is 3.6 or 3.7
*For example for CUDA 10.0 and Python 3.7:*
```bash
conda install -c blazingsql-nightly/label/cuda10.0 -c blazingsql-nightly -c rapidsai-nightly -c nvidia -c conda-forge -c defaults blazingsql python=3.7
```

# Build/Install from Source (Conda Environment)
This is the recommended way of building all of the BlazingSQL components and dependencies from source. It ensures that all the dependencies are available to the build process.

## Stable Version

### Install build dependencies
```bash
conda create -n bsql python=$PYTHON_VERSION
conda activate bsql
conda install --yes -c conda-forge openjdk=8.0 maven cmake gtest gmock rapidjson cppzmq cython=0.29 jpype1 netifaces pyhive
conda install --yes -c conda-forge -c blazingsql bsql-toolchain
conda install --yes -c rapidsai -c nvidia -c conda-forge -c defaults cudf=0.14 dask-cudf=0.14 dask-cuda=0.14 ucx-py=0.14 cudatoolkit=$CUDA_VERSION
```
Where $CUDA_VERSION is 10.0, 10.1 or 10.2 and $PYTHON_VERSION is 3.6 or 3.7
*For example for CUDA 10.0 and Python 3.7:*
```bash
conda create -n bsql python=3.7
conda activate bsql
conda install --yes -c conda-forge openjdk=8.0 maven cmake gtest gmock rapidjson cppzmq cython=0.29 jpype1 netifaces pyhive
conda install --yes -c conda-forge -c blazingsql bsql-toolchain
conda install --yes -c rapidsai -c nvidia -c conda-forge -c defaults cudf=0.14 dask-cudf=0.14 dask-cuda=0.14 ucx-py=0.14 cudatoolkit=10.0
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
```bash
conda create -n bsql python=$PYTHON_VERSION
conda activate bsql

conda install --yes -c conda-forge google-cloud-cpp ninja
conda install --yes -c rapidsai-nightly -c nvidia -c conda-forge -c defaults dask-cuda=0.15 dask-cudf=0.15 ucx-py=0.15 cudf=0.15 python=3.7 cudatoolkit=$CUDA_VERSION
conda install --yes -c conda-forge cmake gtest gmock cppzmq cython=0.29 openjdk=8.0 maven thrift=0.13.0 jpype1 netifaces pyhive
```
Where $CUDA_VERSION is 10.0, 10.1 or 10.2 and $PYTHON_VERSION is 3.6 or 3.7
*For example for CUDA 10.0 and Python 3.7:*
```bash
conda create -n bsql python=3.7
conda activate bsql
conda install --yes -c conda-forge google-cloud-cpp ninja
conda install --yes -c rapidsai-nightly -c nvidia -c conda-forge -c defaults dask-cuda=0.15 dask-cudf=0.15 ucx-py=0.15 cudf=0.15 python=3.7 cudatoolkit=10.0
conda install --yes -c conda-forge cmake gtest gmock cppzmq cython=0.29 openjdk=8.0 maven thrift=0.13.0 jpype1 netifaces pyhive
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

# Documentation
User guides and public APIs documentation can be found at [here](https://docs.blazingdb.com/docs)

Our internal code architecture can be built using Spinx.
```bash
pip install recommonmark
cd $CONDA_PREFIX
cd blazingsql/docs
make html
```
The generated documentation can be viewed in a browser at `blazingsql/docs/_build/html/index.html`


# Community
## Contributing
Have questions or feedback? Post a [new github issue](https://github.com/blazingdb/blazingsql/issues/new/choose).

Please see our [guide for contributing to BlazingSQL](CONTRIBUTING.md).

## Contact
Feel free to join our channel (#blazingsql) in the RAPIDS-GoAi Slack: [![join RAPIDS-GoAi workspace](https://badgen.net/badge/slack/RAPIDS-GoAi/purple?icon=slack)](https://join.slack.com/t/rapids-goai/shared_invite/enQtMjE0Njg5NDQ1MDQxLTJiN2FkNTFkYmQ2YjY1OGI4NTc5Y2NlODQ3ZDdiODEwYmRiNTFhMzNlNTU5ZWJhZjA3NTg4NDZkMThkNTkxMGQ).

You can also email us at [info@blazingsql.com](info@blazingsql.com) or find out more details on [BlazingSQL.com](https://blazingsql.com).

## License
[Apache License 2.0](LICENSE)

## RAPIDS AI - Open GPU Data Science

The RAPIDS suite of open source software libraries aim to enable execution of end-to-end data science and analytics pipelines entirely on GPUs. It relies on NVIDIA® CUDA® primitives for low-level compute optimization, but exposing that GPU parallelism and high-bandwidth memory speed through user-friendly Python interfaces.

## Apache Arrow on GPU

The GPU version of [Apache Arrow](https://arrow.apache.org/) is a common API that enables efficient interchange of tabular data between processes running on the GPU. End-to-end computation on the GPU avoids unnecessary copying and converting of data off the GPU, reducing compute time and cost for high-performance analytics common in artificial intelligence workloads. As the name implies, cuDF uses the Apache Arrow columnar data format on the GPU. Currently, a subset of the features in Apache Arrow are supported.
