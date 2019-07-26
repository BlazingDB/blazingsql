# <div align="left"><img src="img/blazingSQL.png" width="200px"/>&nbsp;BlazingSQL</div>

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

Too see all the ways you can get started with BlazingSQL checkout out our [Getting Started Page](https://blazingsql.com/#/getstarted)


## Build/Install from Source
See build [instructions](CONTRIBUTING.md#setting-up-your-build-environment).

## Contributing

Please see our [guide for contributing to cuDF](CONTRIBUTING.md).

## Contact

To contact us you may email us at [info@blazingsql.com](info@blazingsql.com) or find out more details on the [BlazingSQL site](https://blazingsql.com)

## <div align="left"><img src="img/rapids_logo.png" width="265px"/></div> Open GPU Data Science

The RAPIDS suite of open source software libraries aim to enable execution of end-to-end data science and analytics pipelines entirely on GPUs. It relies on NVIDIA® CUDA® primitives for low-level compute optimization, but exposing that GPU parallelism and high-bandwidth memory speed through user-friendly Python interfaces.

<p align="center"><img src="img/rapids_arrow.png" width="80%"/></p>

### Apache Arrow on GPU

The GPU version of [Apache Arrow](https://arrow.apache.org/) is a common API that enables efficient interchange of tabular data between processes running on the GPU. End-to-end computation on the GPU avoids unnecessary copying and converting of data off the GPU, reducing compute time and cost for high-performance analytics common in artificial intelligence workloads. As the name implies, cuDF uses the Apache Arrow columnar data format on the GPU. Currently, a subset of the features in Apache Arrow are supported.

