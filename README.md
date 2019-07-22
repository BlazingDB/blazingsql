# <div align="left"><img src="img/blazingSQL.png" width="90px"/>&nbsp;blazingSQL</div>

**NOTE:** For the latest stable [README.md](https://github.com/blazingdb/pyblazing/blob/develop/README.md) ensure you are on the `develop` branch.

Built on [RAPIDS AI](https://rapids.ai/), BlazingSQL provides a SQL interface to ETL massive datasets directly into GPU memory and the RAPIDS AI Ecosystem.

For example:
```from blazingsql import BlazingContext
bc = BlazingContext()

# Create Table from CSV
bc.create_table('taxi', '/blazingdb/data/taxi.csv', delimiter= ',', names = column_names)

# Query
result = bc.sql('SELECT count(*) FROM main.taxi GROUP BY year(key)').get()
result_gdf = result.columns

#Print GDF 
print(result_gdf)
```


For additional information, browse our complete [documentation](https://docs.blazingdb.com/docs/)

## Quick Start

Too see all the ways you can get started with BlazingSQL checkout out our [Getting Started Page](https://blazingdb.com/#/getstarted)


## Build/Install from Source
See build [instructions](CONTRIBUTING.md#setting-up-your-build-environment).

## Contributing

Please see our [guide for contributing to cuDF](CONTRIBUTING.md).

## Contact

Find out more details on the [RAPIDS site](https://rapids.ai/community.html)

## <div align="left"><img src="img/rapids_logo.png" width="265px"/></div> Open GPU Data Science

The RAPIDS suite of open source software libraries aim to enable execution of end-to-end data science and analytics pipelines entirely on GPUs. It relies on NVIDIA® CUDA® primitives for low-level compute optimization, but exposing that GPU parallelism and high-bandwidth memory speed through user-friendly Python interfaces.

<p align="center"><img src="img/rapids_arrow.png" width="80%"/></p>

### Apache Arrow on GPU

The GPU version of [Apache Arrow](https://arrow.apache.org/) is a common API that enables efficient interchange of tabular data between processes running on the GPU. End-to-end computation on the GPU avoids unnecessary copying and converting of data off the GPU, reducing compute time and cost for high-performance analytics common in artificial intelligence workloads. As the name implies, cuDF uses the Apache Arrow columnar data format on the GPU. Currently, a subset of the features in Apache Arrow are supported.



####################################################################################################################

![](http://www.blazingdb.com/images/Logo_Blazing_verde.png)

# BlazingDB Python Connector

BlazingDB Python Connector allow to connector to BlazingDB completely in Python.

## Requirements

- CUDA >=9.1
- python 2.7
- python-pip 2.7
- python-setuptools
- wheel (>= 0.29)
- requests (>= 2.12)
- pyarrow (>= 0.9)

For Ubuntu 16.04 you can install the required dependencies with:

```shell-script
sudo apt-get install -y --no-install-recommends python python-pip python-setuptools flex bison libffi-dev
pip install numpy==1.15.2
pip install wheel
pip install requests
pip install pyarrow
```

## Installation

You can install directly from sources with:

```shell-script
pip install .
```

## Usage

Hello world:

```py
from blazingdb import Connection
from blazingdb import Connector
from blazingdb import to_arrow_table

blazing_connection = Connection('http://127.0.0.1:8080/', 'admin', '123456', 'database_name')
blazing_connector = Connector(blazing_connection)
connection_status = blazing_connector.connect()

arrow_bytes = blazing_connector.run('select * from orders limit 10', connection_status)
arrow_table = to_arrow_table(arrow_bytes)

print "num_columns: " + str(arrow_table.num_columns)
print "num_rows: " + str(arrow_table.num_rows)
print "shape: " + str(arrow_table.shape)
print "schema: " + str(arrow_table.schema)
```

## Examples
See [examples](examples)

## More BlazingDB documentation
Please visit your [SQL Guide](https://blazingdb.readme.io/docs/blazingdb-sql-guide) for more information about query structures and examples.

## Author

BlazingDB Inc. ([www.blazingdb.com](http://www.blazingdb.com))
