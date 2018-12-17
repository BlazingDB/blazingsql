import pyarrow as pa
import pyblazing
from pyblazing import DriverType, FileSystemType, EncryptionType
from pyblazing import SchemaFrom

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


# def load_from_pandas():
#     column_names = ["n_nationkey", "n_name", "n_regionkey", "n_comments"]
#     column_types = ["int32", "int64", "int32", "int64"]
#     nation_df = pd.read_csv("data/nation.psv", delimiter='|', names=column_names)
#     nation_df = nation_df[['n_nationkey', 'n_regionkey']]
#     nation_gdf = cudf.DataFrame.from_pandas(nation_df)
#
#     nation_schema = Schema(type=SchemaFrom.Gdf, gdf=nation_gdf)
#     gdf_nation_table = pyblazing.register_table('nation', nation_schema)
#

def load_and_query_csv_files():
    print('*** Register a POSIX File System ***')
    fs_status = pyblazing.register_file_system(
        authority="tpch",
        type=FileSystemType.POSIX,
        root="/"  # como se usa en ral
    )
    print(fs_status)
    names=['n_nationkey', 'n_name', 'n_regionkey', 'n_comment']
    dtypes = [3, 4, 3, 4]
    #todo, estos kwargs deberian coincidir con flatbuffers CsvFile
        # path: string;
        # delimiter: string;
        # lineTerminator: string;
        # skipRows: int;
        # names: [string];
        # dtypes: [int];
    nation_schema = pyblazing.register_table_schema(table_name='nation_csv', type=SchemaFrom.CsvFile, path='/tmp/tpch/1mb/nation.psv', delimiter='|', dtypes=dtypes, names=names)
    # query using multiples files
    sql_data = {
        nation_schema: ['/tmp/tpch/1mb/nation.psv']
    }
    sql = 'select n_nationkey, n_regionkey + n_nationkey as addition from main.nation_csv'
    result_gdf = pyblazing.run_query_filesystem(sql, sql_data)
    print(sql)
    print(result_gdf)

    fs_status = pyblazing.deregister_file_system(authority="tpch")
    print(fs_status)
    fs_status = pyblazing.deregister_file_system(authority="tpch")
    print(fs_status)

def load_and_query_parquet_files():
    print('*** Register a POSIX File System ***')
    fs_status = pyblazing.register_file_system(
        authority="tpch",
        type=FileSystemType.POSIX,
        root="/" #como se usa en ral
    )
    print(fs_status)

    # register schema
    orders_schema = pyblazing.register_table_schema(table_name='customer_parquet', type=SchemaFrom.ParquetFile, path='/tmp/DataSet50mb/customer_0_0.parquet')

    # query using multiples files
    sql_data = {
        orders_schema: ['/tmp/DataSet50mb/customer_0_0.parquet',
                        '/tmp/DataSet50mb/customer_0_1.parquet']
    }
    sql = 'select c_custkey, c_acctbal, c_acctbal + c_nationkey as addition from main.customer_parquet'
    result_gdf = pyblazing.run_query_filesystem(sql, sql_data)
    print(sql)
    print(result_gdf)

    fs_status = pyblazing.deregister_file_system(authority="tpch")
    print(fs_status)

if __name__ == '__main__':
    # load_and_query_parquet_files()
    load_and_query_csv_files()