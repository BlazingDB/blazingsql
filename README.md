![](http://www.blazingdb.com/images/Logo_Blazing_verde.png)

# BlazingDB Python Connector

BlazingDB Python Connector allow to connector to BlazingDB completely in Python.

## Requirements

- python 2.7
- python-pip 2.7
- python-setuptools
- wheel (>= 0.29)
- requests (>= 2.12)
- pyarrow (>= 0.9)

- numpy 1.15.2

For Ubuntu 16.04 you can install the required dependencies with:

```shell-script
sudo apt-get install -y --no-install-recommends python python-pip python-setuptools
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
