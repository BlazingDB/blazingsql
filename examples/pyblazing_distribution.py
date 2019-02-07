import pyblazing
from pyblazing import SchemaFrom


def main():
    customer_schema = pyblazing.register_table_schema(table_name='customer_parquet',
                                                      type=SchemaFrom.ParquetFile,
                                                      path='/tmp/DataSet50mb/customer_0_0.parquet')

    sql_data = {
        customer_schema: ['/tmp/DataSet50mb/customer_0_0.parquet',
                          '/tmp/DataSet50mb/customer_0_1.parquet']
    }

    sql_query = '''select * from main.customer_parquet'''

    result = pyblazing.run_query_filesystem(sql_query, sql_data)

    print(sql_query)
    print(result)


if __name__ == '__main__':
    main()
