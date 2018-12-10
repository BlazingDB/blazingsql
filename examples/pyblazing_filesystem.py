import pyarrow as pa
import pyblazing
from pyblazing import DriverType, FileSystemType, EncryptionType
from pyblazing import Schema, SchemaFrom, Table, SqlData, SqlDataCollection

def register_hdfs():
    print('*** Register a HDFS File System ***')
    fs_status = pyblazing.register_file_system(
        authority="tpch2",
        type=FileSystemType.HDFS,
        root="/tmp/tpch/1mb/",
        params={
            "host": "192.168.0.1",
            "port": 8080,
            "user": "aocsa",
            "driverType": DriverType.LIBHDFS3,
            "kerberosTicket": ""
        }
    )
    print(fs_status)


def register_s3():
    print('*** Register a S3 File System ***')
    fs_status = pyblazing.register_file_system(
        authority="tpch2",
        type=FileSystemType.S3,
        root="/tmp/tpch/1mb/",
        params={
            "fs_name": "company",
            "bucket_name": "company_bucket",
            "encryption_type": EncryptionType.NONE,
            "kms_key_amazon_resource_name": "",
            "access_key_id": "accessKeyIddsf3",
            "secret_key": "secretKey234",
            "session_token": ""
        }
    )
    print(fs_status)

import numpy as np
import pandas as pd
import cudf as cudf

def main():
    customer_schema = pyblazing.Schema(type=SchemaFrom.ParquetFile, path='/tpch/orders00.parquet')
    parquet_customer_table = pyblazing.create_table(table_name='order', schema=customer_schema)

    column_names = ["n_nationkey", "n_name", "n_regionkey", "n_comments"]
    column_types = ["int32", "int64", "int32", "int64"]
    nation_df = pd.read_csv("data/nation.psv", delimiter='|', names=column_names)
    nation_df = nation_df[['n_nationkey', 'n_regionkey']]
    nation_gdf = cudf.DataFrame.from_pandas(nation_df)

    order_schema = Schema(type=SchemaFrom.CsvFile, path=['/tmp/order.psv'], delimiter='|', dtype=column_types, names=column_names)
    csv_order_table = pyblazing.create_table(table_name='order', schema=order_schema)
    nation_schema = Schema(type=SchemaFrom.Gdf, gdf=nation_gdf)
    gdf_nation_table = pyblazing.create_table('nation', nation_schema)

    # single query
    sql = 'select * from order, customer, nation'
    sql_data = {
        gdf_nation_table: [],
        parquet_customer_table: ['/tpch/customer00.parquet'],
        csv_order_table:['/tpch/order00.csv']
    }
    result = pyblazing.run_sql(sql, sql_data)

    # # multiples queries
    # sql = 'select * from order, customer, nation'
    # sql_data = SqlDataCollection(tables=[gdf_nation_table, parquet_customer_table, csv_order_table],
    #                    list_of_files=[
    #                        [None, '/tpch/customer00.parquet', '/tpch/order00.csv'],
    #                        [None, '/tpch/customer01.parquet', '/tpch/order01.csv']
    #                    ])
    # run_query = lambda sql_data: pyblazing.run_sql(sql, sql_data)
    # results = map(run_query, sql_data.list_of_files)


if __name__ == '__main__':
    main()
