import numpy as np
import pandas as pd
import cudf as cudf
import pyblazing
from pyblazing import DriverType, FileSystemType, EncryptionType
from pyblazing import SchemaFrom


def register_hdfs():
    print('*** Register a HDFS File System ***')
    fs_status = pyblazing.register_file_system(
        authority="tpch_hdfs",
        type=FileSystemType.HDFS,
        root="/",
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
        authority="tpch_s3",
        type=FileSystemType.S3,
        root="/",
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


def main():
    print('*** Register a POSIX File System ***')
    fs_status = pyblazing.register_file_system(
        authority="tpch",
        type=FileSystemType.POSIX,
        root="/"
    )
    print(fs_status)

    customer_schema = pyblazing.register_table_schema(table_name='customer_parquet', type=SchemaFrom.ParquetFile, path='/tmp/DataSet50mb/customer_0_0.parquet')
    nation_schema = pyblazing.register_table_schema(table_name='nation_parquet', type=SchemaFrom.ParquetFile,
                                                      path='/tmp/DataSet50mb/nation_0_0.parquet')


    region_schema = pyblazing.register_table_schema(table_name='region_parquet', type=SchemaFrom.ParquetFile, path='/tmp/DataSet50mb/region_0_0.parquet')

    # query using multiples files
    sql_data = {
        customer_schema: ['/tmp/DataSet50mb/customer_0_0.parquet',
                          '/tmp/DataSet50mb/customer_0_1.parquet'],
        nation_schema: ['/tmp/DataSet50mb/nation_0_0.parquet']
    }

    sql = '''
        select avg(c.c_custkey), avg(c.c_nationkey), n.n_regionkey
        from main.customer_parquet as c
        inner join main.nation_parquet as n
        on c.c_nationkey = n.n_nationkey
        group by n.n_regionkey
    '''

    result_gdf = pyblazing.run_query_filesystem(sql, sql_data)
    print(sql)
    print(result_gdf)

    sql = 'select * from main.nation_parquet'
    sql_data = {
        nation_schema: ['/tmp/DataSet50mb/nation_0_0.parquet']
    }
    result_gdf = pyblazing.run_query_filesystem(sql, sql_data)
    print(sql)
    print(result_gdf)



    sql = 'select r_regionkey from main.region_parquet'
    sql_data = {
        region_schema: ['/tmp/DataSet50mb/region_0_0.parquet']
    }
    result_gdf = pyblazing.run_query_filesystem(sql, sql_data)
    print(sql)
    print(result_gdf)

    fs_status = pyblazing.deregister_file_system(authority="tpch")
    print(fs_status)

if __name__ == '__main__':
    main()
