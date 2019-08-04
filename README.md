> A lightweight, GPU accelerated, SQL engine built on the [RAPIDS.ai](https://rapids.ai) ecosystem.

[![BlazingSQL](https://github.com/BlazingDB/pyBlazing/blob/develop/img/blazingSQL.png)](https://colab.research.google.com/drive/1r7S15Ie33yRw8cmET7_bjCpvjJiDOdub)


[Getting Started](https://github.com/BlazingDB/pyBlazing#getting-started) | [Documentation](https://docs.blazingdb.com) | [Examples](https://github.com/BlazingDB/pyBlazing#examples) | [Contributing](https://github.com/BlazingDB/pyBlazing#contributing) | [License](https://github.com/BlazingDB/pyBlazing/blob/develop/LICENSE) | [Blog](https://blog.blazingdb.com)

BlazingSQL is a GPU accelerated SQL engine built on top of the RAPIDS ecosystem. RAPIDS is based on the [Apache Arrow](http://arrow.apache.org) columnar memory format, and [cuDF](https://github.com/rapidsai/cudf) is a GPU DataFrame library for loading, joining, aggregating, filtering, and otherwise manipulating data.

BlazingSQL is a SQL interface for cuDF, with various features to support large scale data science workflows and enterprise datasets.
* **Query Data Stored Externally** - a single line of code can register remote storage solutions, such as Amazon S3.
* **Simple SQL** - incredibly easy to use, run a SQL query and the results are GPU DataFrames (GDFs).
* **Interoperable** - GDFs are immediately accessible to any [RAPIDS](htts://github.com/rapidsai) library for data science workloads.

Check out our 5-min quick start notebook [![Google Colab Badge](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/drive/1r7S15Ie33yRw8cmET7_bjCpvjJiDOdub) using BlazingSQL.

## Getting Started

Please reference our [docs](https://docs.blazingdb.com/docs/blazingsql) to find out how to install BlazingSQL.

Querying a CSV file in Amazon S3 with BlazingSQL:

For example:
```python
from blazingsql import BlazingContext
bc = BlazingContext()

bc.s3('dir_name', bucket_name='bucket_name', access_key_id='access_key', secrect_key='secret_key')

# Create Table from CSV
bc.create_table('taxi', '/dir_name/taxi.csv')

# Query
result = bc.sql('SELECT count(*) FROM taxi GROUP BY year(key)').get()
result_gdf = result.columns

#Print GDF 
print(result_gdf)
```
## Examples

* Getting Started Guide - [Google Colab](https://colab.research.google.com/drive/1r7S15Ie33yRw8cmET7_bjCpvjJiDOdub#scrollTo=14GwxmLsTV_p)
* Netflow Demo - [Google Colab](https://colab.research.google.com/drive/1RYOYthqxUl922LYMAuNneKgmWB8YGTKB)
* Taxi cuML Linear Regression - [Google Colab](https://colab.research.google.com/drive/10il0C55uRhsgu2vqRVLqdB7Zp0gDt8Me)

## Documentation
You can find our full documentation at [the following site](https://docs.blazingdb.com/docs/)

## Build/Install from Source
See build [instructions](CONTRIBUTING.md#setting-up-your-build-environment).

## Contributing
Have questions or feedback? Post a [new github issue](https://github.com/BlazingDB/pyBlazing/issues/new/choose).

Please see our [guide for contributing to BlazingSQL](CONTRIBUTING.md).

## Contact
Feel free to join our Slack chat room: [RAPIDS Slack Channel](https://join.slack.com/t/rapids-goai/shared_invite/enQtMjE0Njg5NDQ1MDQxLTViZWFiYTY5MDA4NWY3OWViODg0YWM1MGQ1NzgzNTQwOWI1YjE3NGFlOTVhYjQzYWQ4YjI4NzljYzhiOGZmMGM)

You may also email us at [info@blazingsql.com](info@blazingsql.com) or find out more details on the [BlazingSQL site](https://blazingsql.com)

## License
[Apache License 2.0](https://github.com/BlazingDB/pyBlazing/blob/develop/LICENSE)

## RAPIDS AI - Open GPU Data Science

The RAPIDS suite of open source software libraries aim to enable execution of end-to-end data science and analytics pipelines entirely on GPUs. It relies on NVIDIA® CUDA® primitives for low-level compute optimization, but exposing that GPU parallelism and high-bandwidth memory speed through user-friendly Python interfaces.

<p align="center"><img src="https://github.com/rapidsai/cudf/raw/branch-0.9/img/rapids_arrow.png" width="70%"/></p>

## Apache Arrow on GPU

The GPU version of [Apache Arrow](https://arrow.apache.org/) is a common API that enables efficient interchange of tabular data between processes running on the GPU. End-to-end computation on the GPU avoids unnecessary copying and converting of data off the GPU, reducing compute time and cost for high-performance analytics common in artificial intelligence workloads. As the name implies, cuDF uses the Apache Arrow columnar data format on the GPU. Currently, a subset of the features in Apache Arrow are supported.
