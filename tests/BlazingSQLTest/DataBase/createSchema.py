import os
import re
import tempfile
from collections import OrderedDict
from os import listdir
from os.path import isdir
import glob
from enum import IntEnum, unique

import cudf
import dask_cudf
import numpy as np
import pandas as pd
import pyblazing
from blazingsql import DataType
from pyhive import hive

from Configuration import Settings as Settings
from DemoTest.chronometer import Chronometer


SQLEngineStringDataTypeMap = {
    DataType.MYSQL: "mysql",
    DataType.SQLITE: "sqlite",
    DataType.POSTGRESQL: "postgresql",
    # TODO percy c.gonzales support for more db engines
}

# ************** READ TPCH DATA AND CREATE TABLES ON DRILL ****************

tpchTables = [
    "customer",
    "orders",
    "supplier",
    "lineitem",
    "part",
    "partsupp",
    "nation",
    "region",
]
# extraTables = ["perf", "acq", "names", "bool_orders"]
extraTables = ["bool_orders"]
tableNames = tpchTables + extraTables

smilesTables = ["docked", "dcoids", "smiles", "split"]


class sql_connection:
    def __init__(self, **kwargs):
        hostname = kwargs.get("hostname", "")
        port = kwargs.get("port", 0)
        username = kwargs.get("username", "")
        password = kwargs.get("password", "")
        schema = kwargs.get("schema", "")

        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.schema = schema


def get_sql_connection(fileSchemaType: DataType):
    sql_hostname = os.getenv("BLAZINGSQL_E2E_SQL_HOSTNAME", "")
    if fileSchemaType in [DataType.MYSQL, DataType.POSTGRESQL]:
        if not sql_hostname: return None

    sql_port = int(os.getenv("BLAZINGSQL_E2E_SQL_PORT", 0))
    if fileSchemaType in [DataType.MYSQL, DataType.POSTGRESQL]:
        if sql_port == 0: return None

    sql_username = os.getenv("BLAZINGSQL_E2E_SQL_USERNAME", "")
    if fileSchemaType in [DataType.MYSQL, DataType.POSTGRESQL]:
        if not sql_username: return None

    sql_password = os.getenv("BLAZINGSQL_E2E_SQL_PASSWORD", "")
    if fileSchemaType in [DataType.MYSQL, DataType.POSTGRESQL]:
        if not sql_password: return None

    sql_schema = os.getenv("BLAZINGSQL_E2E_SQL_SCHEMA", "")
    if not sql_schema: return None

    ret = sql_connection(hostname = sql_hostname,
                         port = sql_port,
                         username = sql_username,
                         password = sql_password,
                         schema = sql_schema)
    return ret


def getFiles_to_tmp(tpch_dir, n_files, ext):
    list_files = []
    for name in tableNames:
        list_tmp = get_filenames_table(name, tpch_dir, n_files,
                                ext=ext,full_path=False)
        list_files = np.append(list_files, list_tmp)
    dirpath = tempfile.mkdtemp()
    count = 0
    for item in list_files:
        dataFileTokens = item.split(".")
        dataFileName = dataFileTokens[0]
        os.symlink(
            tpch_dir + item, dirpath + "/" + dataFileName +
            "_" + str(count) + "." + ext
        )
        count = count + 1

    return dirpath


def get_spark_schema(table, nullable):
    column_names = get_column_names(table)
    column_types = get_dtypes(table)

    schema = st.StructType()
    for name, type in zip(column_names, column_types):
        schema.add(name, get_dtypes_spark(type), nullable)
    return schema


def init_spark_schema(spark, dir_data_lc, **kwargs):

    smiles_test = kwargs.get("smiles_test", False)
    testsWithNulls = Settings.data["RunSettings"]["testsWithNulls"]

    if smiles_test:
        dir_data_lc = dir_data_lc + "smiles/"
        table_names=smilesTables
    else:
        if testsWithNulls == "true":
            dir_data_lc = dir_data_lc + "tpch-with-nulls/"
        else:
            dir_data_lc = dir_data_lc + "tpch/"
        table_names=tpchTables

    for name in table_names:
        spark.sql("DROP TABLE IF EXISTS `%(table)s`" % {"table": name})

    dir_path = os.path.dirname(os.path.realpath(__file__))

    fileSchemaType = kwargs.get("fileSchemaType")
    if fileSchemaType is not None:
        ext = get_extension(fileSchemaType)
    else:
        ext = "orc"

    num_files = kwargs.get("n_files")
    if num_files is not None:
        dir_data_lc = getFiles_to_tmp(dir_data_lc, num_files, ext)

    nullable = True

    bool_test = kwargs.get("bool_test", None)
    if bool_test and testsWithNulls != "true":
        bool_orders_df = spark.read.orc(dir_data_lc + "/bool_orders_*.psv")
        bool_orders_df.createOrReplaceTempView("bool_orders")

    for table_name in table_names:
        if ext == "psv":
            table_name_schema = get_spark_schema(table_name, nullable)
            table_name_df = spark.read.load(
                dir_data_lc + "/" + table_name + "_*." + str(ext),
                format="csv",
                sep="|",
                schema=table_name_schema,
            )
        elif ext == "parquet":
            table_name_df = spark.read.parquet(
                dir_data_lc + "/" + table_name + "_*." + str(ext)
            )
        else:
            table_name_df = spark.read.orc(
                dir_data_lc + "/" + table_name + "_*." + str(ext)
            )

        table_name_df.createOrReplaceTempView(table_name)


def init_hive_schema(drill, cursor, dir_data_lc, **kwargs):
    timeout = 300

    dir_path = os.path.dirname(os.path.realpath(__file__))

    smiles_test = kwargs.get("smiles_test", False)
    if smiles_test:
        dir_data_lc = dir_data_lc + "smiles/"
        table_names=smilesTables
    else:
        dir_data_lc = dir_data_lc + "tpch/"
        table_names=tableNames

    for name in table_names:
        cursor.execute(
            "DROP TABLE IF EXISTS %(table)s PURGE" % {"table": name})
        print("DROP TABLE IF EXISTS %(table)s" % {"table": name})

    # cursor.execute('select * from customer')
    # print(cursor.fetchall())
    # exit()

    fileSchemaType = kwargs.get("fileSchemaType")
    if fileSchemaType is not None:
        ext = get_extension(fileSchemaType)
    else:
        ext = "orc"

    num_files = kwargs.get("n_files")
    if num_files is not None:
        dir_data_lc = getFiles_to_tmp(dir_data_lc, num_files, ext)

    bool_test = kwargs.get("bool_test", None)
    if bool_test:
        drill.query(
            """
        create table dfs.tmp.`bool_orders/` as
            select CASE WHEN columns[0] = '' OR columns[0] = 'null' THEN null
                ELSE cast(columns[0] as bigint) END as o_orderkey,
                CASE WHEN columns[1] = '' OR columns[1] = 'null' THEN null
                ELSE cast(columns[1] as int) END as o_custkey,
                columns[2] as o_orderstatus,
                CASE WHEN columns[3] = '' OR columns[3] = 'null' THEN null
                ELSE cast(columns[3] as double) END as o_totalprice,
                columns[4] as o_orderdate,
                columns[5] as o_orderpriority,
                columns[6] as o_clerk,
                CASE WHEN columns[7] = '' OR columns[7] = 'null' THEN null
                ELSE cast(columns[7] as int) END as o_shippriority,
                columns[8] as o_comment,
                CASE WHEN columns[9] = '' OR columns[9] = 'null' THEN null
                ELSE cast(columns[9] as boolean) END as o_confirmed
        FROM table(dfs.`%(dir_data_lc)s/bool_orders_*.psv`
         (type => 'text', fieldDelimiter => '|'))
        """
            % {"dir_data_lc": dir_data_lc},
            timeout,
        )

    for table_name in table_names:
        column_names = get_column_names(table_name, bool_test)
        data_types = get_dtypes(table_name, bool_test)

        names_types = zip(column_names, data_types)

        column_list = ", ".join(
            [name + " " + get_dtypes_hive(type) for name, type in names_types]
        )

        if ext == "orc":
            cursor.execute(
                """CREATE EXTERNAL TABLE %(table_name)s
                            ( %(column_list)s ) STORED AS ORC"""
                % {"table_name": table_name}
            )

            cursor.execute(
                """LOAD DATA INPATH 'hdfs:%(dir_data_lc)s/%(table_name)s_*.orc'
                            INTO TABLE %(table_name)s"""
                % {"dir_data_lc": dir_data_lc, "table_name": table_name}
            )
        elif ext == "parquet":
            cursor.execute(
                """CREATE EXTERNAL TABLE %(table_name)s
                            ( %(column_list)s ) STORED AS PARQUET"""
                % {"table_name": table_name}
            )

            cursor.execute(
                """LOAD DATA INPATH
                           'hdfs:%(dir_data_lc)s/%(table_name)s_*.parquet'
                            INTO TABLE %(table_name)s"""
                % {"dir_data_lc": dir_data_lc, "table_name": table_name}
            )

            # cursor.execute('DROP TABLE TRANSACTIONS')

            # cursor.execute('''CREATE TABLE TRANSACTIONS(
            #             t_person_id BIGINT,
            #             t_company_id INT,
            #             t_amount BIGINT,
            #             t_year INT
            #             )
            #             STORED AS PARQUET''')

            # cursor.execute('''LOAD DATA INPATH
            #     'hdfs:/home/kharoly/blazingsql/DataSet100MB_2/transactions/
            #      transactions.parquet'
            #      OVERWRITE INTO TABLE TRANSACTIONS''')

            # cursor.execute('''drop table PTRANSACTIONS;
            #     CREATE TABLE PTRANSACTIONS(
            #     t_person_id INT,
            #     t_amount INT
            #     )
            #     PARTITIONED BY (t_year INT, t_company_id INT)
            #     STORED AS PARQUET''')

            # cursor.execute('''INSERT OVERWRITE TABLE PTRANSACTIONS
            #        PARTITION (t_year=2017, t_company_id=1)
            #        SELECT t_person_id, t_amount FROM TRANSACTIONS
            #        WHERE t_year=2017 and t_company_id=1''')

            # cursor.execute('''SHOW PARTITIONS PTRANSACTIONS''')

            # break
        elif ext == "psv":
            cursor.execute(
                """CREATE EXTERNAL TABLE %(table_name)s
                            ( %(column_list)s ) ROW FORMAT DELIMITED FIELDS
                            TERMINATED BY '|' STORED AS TEXTFILE"""
                % {"table_name": table_name, "column_list": column_list}
            )

            cursor.execute(
                """LOAD DATA INPATH 'hdfs:%(dir_data_lc)s/%(table_name)s_*.psv'
                            INTO TABLE %(table_name)s"""
                % {"dir_data_lc": dir_data_lc, "table_name": table_name}
            )


def init_drill_schema(drill, dir_data_lc, **kwargs):
    timeout = 300

    smiles_test = kwargs.get("smiles_test", False)
    testsWithNulls = Settings.data["RunSettings"]["testsWithNulls"]

    if smiles_test:
        dir_data_lc = dir_data_lc + "smiles/"
        table_names=smilesTables
    else:
        if testsWithNulls == "true":
            dir_data_lc = dir_data_lc + "tpch-with-nulls/"
        else:
            dir_data_lc = dir_data_lc + "tpch/"
        table_names=tpchTables

    for name in table_names:
        drill.query(
            "DROP TABLE IF EXISTS " + "dfs.tmp.`%(table)s`"
            % {"table": name}, timeout
        )

    num_files = kwargs.get("n_files")
    if num_files is not None:
        dir_data_lc = getFiles_to_tmp(dir_data_lc, num_files, 'psv')

    bool_test = kwargs.get("bool_test", None)
    if bool_test:
        drill.query(
            "DROP TABLE IF EXISTS " + "dfs.tmp.`%(table)s`"
            % {"table": "bool_orders"}, timeout
        )

        drill.query(
            """
        create table dfs.tmp.`bool_orders/` as
            select CASE WHEN columns[0] = '' OR columns[0] = 'null' THEN null
                ELSE cast(columns[0] as bigint) END as o_orderkey,
                CASE WHEN columns[1] = '' OR columns[1] = 'null' THEN null
                ELSE cast(columns[1] as int) END as o_custkey,
                columns[2] as o_orderstatus,
                CASE WHEN columns[3] = '' OR columns[3] = 'null' THEN null
                ELSE cast(columns[3] as double) END as o_totalprice,
                columns[4] as o_orderdate,
                columns[5] as o_orderpriority,
                columns[6] as o_clerk,
                CASE WHEN columns[7] = '' OR columns[7] = 'null' THEN null
                ELSE cast(columns[7] as int) END as o_shippriority,
                columns[8] as o_comment,
                CASE WHEN columns[9] = '' OR columns[9] = 'null' THEN null
                ELSE cast(columns[9] as boolean) END as o_confirmed
        FROM table(dfs.`%(dir_data_lc)s/bool_orders_*.psv`
         (type => 'text', fieldDelimiter => '|'))
        """
            % {"dir_data_lc": dir_data_lc},
            timeout,
        )

    for table_name in table_names:
        drill.query(
            """ create table dfs.tmp.`%(table_name)s` as select * FROM
                     table(dfs.`%(dir_data_lc)s/%(table_name)s_*.parquet`
                     (type => 'parquet'))
                    """
            % {"dir_data_lc": dir_data_lc, "table_name": table_name},
            timeout,
        )


def init_drill_mortgage_schema(drill, tpch_dir):

    dir_path = os.path.dirname(os.path.realpath(__file__))
    print(dir_path)

    tableNames = ["perf", "acq", "names"]

    timeout = 200

    for name in tableNames:
        drill.query(
            "DROP TABLE IF EXISTS" + "dfs.tmp.`%(table)s`"
            % {"table": name}, timeout
        )
    drill.query(
        """
        create table dfs.tmp.`perf/` as
          SELECT
            CAST(columns[0] AS BIGINT) as `loan_id`,
            columns[1] as `monthly_reporting_period`,
            columns[2] as `servicer`,
            CAST(columns[3] AS FLOAT) as `interest_rate`,
            CAST(columns[4] AS FLOAT) as `current_actual_upb`,
            CAST(columns[5] AS FLOAT) as `loan_age`,
            CAST(columns[6] AS FLOAT) as `remaining_months_to_legal_maturity`,
            CAST(columns[7] AS FLOAT) as `adj_remaining_months_to_maturity`,
            columns[8] as `maturity_date`,
            CAST(columns[9] AS FLOAT) as `msa`,
            CAST(columns[10] AS INT) as `current_loan_delinquency_status`,
            columns[11] as `mod_flag`,
            columns[12] as `zero_balance_code`,
            columns[13] as `zero_balance_effective_date`,
            columns[14] as `last_paid_installment_date`,
            columns[15] as `foreclosed_after`,
            columns[16] as `disposition_date`,
            CAST(columns[17] AS FLOAT)  as `foreclosure_costs`,
            CAST(columns[18] AS FLOAT)  as
             `prop_preservation_and_repair_costs`,
            CAST(columns[19] AS FLOAT)  as `asset_recovery_costs`,
            CAST(columns[20] AS FLOAT)  as `misc_holding_expenses`,
            CAST(columns[21] AS FLOAT)  as `holding_taxes`,
            CAST(columns[22] AS FLOAT)  as `net_sale_proceeds`,
            CAST(columns[23] AS FLOAT) as `credit_enhancement_proceeds`,
            CAST(columns[24] AS FLOAT)  as `repurchase_make_whole_proceeds`,
            CAST(columns[25] AS FLOAT) as `other_foreclosure_proceeds`,
            CAST(columns[26] AS FLOAT) as `non_interest_bearing_upb`,
            CAST(columns[27] AS FLOAT) as `principal_forgiveness_upb`,
            columns[28] as `repurchase_make_whole_proceeds_flag`,
            CAST(columns[29] AS FLOAT) as
             `foreclosure_principal_write_off_amount`,
            columns[30] as `servicing_activity_indicator`
        FROM table(dfs.`%(tpch_dir)s/perf/Performance_2000Q1.txt`
         (type => 'text', fieldDelimiter => '|'))
        """
        % {"tpch_dir": tpch_dir},
        timeout,
    )

    drill.query(
        """
        create table dfs.tmp.`acq/` as
          SELECT
            CAST(columns[0] AS BIGINT) as `loan_id`,
            columns[1] as `orig_channel`,
            columns[2] as `seller_name`,
            CAST(columns[3] AS FLOAT) as `orig_interest_rate`,
            CAST(columns[4] AS BIGINT) as `orig_upb`,
            CAST(columns[5] AS BIGINT) as `orig_loan_term`,
            columns[6] as `orig_date`,
            columns[7] as `first_pay_date`,
            CAST(columns[8] AS FLOAT) as `orig_ltv`,
            CAST(columns[9] AS FLOAT) as `orig_cltv`,
            CAST(columns[10] AS FLOAT) as `num_borrowers`,
            CAST(columns[11] AS FLOAT) as `dti`,
            CAST(columns[12] AS FLOAT) as `borrower_credit_score`,
            columns[13] as `first_home_buyer`,
            columns[14] as `loan_purpose`,
            columns[15] as `property_type`,
            CAST(columns[16] AS BIGINT) as `num_units`,
            columns[17] as `occupancy_status`,
            columns[18] as `property_state`,
            CAST(columns[19] AS BIGINT) as `zip`,
            CAST(columns[20] AS FLOAT) as `mortgage_insurance_percent`,
            columns[21] as `product_type`,
            CAST(columns[22] AS FLOAT) as `coborrow_credit_score`,
            CAST(columns[23] AS FLOAT) as `mortgage_insurance_type`,
            columns[24] as `relocation_mortgage_indicator`
        FROM table(dfs.`%(tpch_dir)s/acq/Acquisition_2000Q1.txt`
         (type => 'text', fieldDelimiter => '|'))
        """
        % {"tpch_dir": tpch_dir},
        timeout,
    )

    drill.query(
        """create table dfs.tmp.`names/` as SELECT
            columns[0] as `seller_name`,
            columns[1] as `new_seller_name`
         FROM table(dfs.`%(tpch_dir)s/names.load`
         (type => 'text', fieldDelimiter => '|', quote => '"'))
        """
        % {"tpch_dir": tpch_dir},
        timeout,
    )


# ***************** READ DATA FROM TPCH CSV FILES **************************
def get_column_names(table_name, bool_column=False):
    switcher = {
        "customer": [
            "c_custkey",
            "c_name",
            "c_address",
            "c_nationkey",
            "c_phone",
            "c_acctbal",
            "c_mktsegment",
            "c_comment",
        ],
        "region": ["r_regionkey", "r_name", "r_comment"],
        "nation": ["n_nationkey", "n_name", "n_regionkey", "n_comment"],
        "lineitem": [
            "l_orderkey",
            "l_partkey",
            "l_suppkey",
            "l_linenumber",
            "l_quantity",
            "l_extendedprice",
            "l_discount",
            "l_tax",
            "l_returnflag",
            "l_linestatus",
            "l_shipdate",
            "l_commitdate",
            "l_receiptdate",
            "l_shipinstruct",
            "l_shipmode",
            "l_comment",
        ],
        "orders": [
            "o_orderkey",
            "o_custkey",
            "o_orderstatus",
            "o_totalprice",
            "o_orderdate",
            "o_orderpriority",
            "o_clerk",
            "o_shippriority",
            "o_comment",
        ],
        "supplier": [
            "s_suppkey",
            "s_name",
            "s_address",
            "s_nationkey",
            "s_phone",
            "s_acctbal",
            "s_comment",
        ],
        "part": [
            "p_partkey",
            "p_name",
            "p_mfgr",
            "p_brand",
            "p_type",
            "p_size",
            "p_container",
            "p_retailprice",
            "p_comment",
        ],
        "partsupp": [
            "ps_partkey",
            "ps_suppkey",
            "ps_availqty",
            "ps_supplycost",
            "ps_comment",
        ],
    }

    if bool_column:
        switcher.update(
            {
                "bool_orders": [
                    "o_orderkey",
                    "o_custkey",
                    "o_orderstatus",
                    "o_totalprice",
                    "o_orderdate",
                    "o_orderpriority",
                    "o_clerk",
                    "o_shippriority",
                    "o_comment",
                    "o_confirmed",
                ]
            }
        )

    # Get the function from switcher dictionary
    func = switcher.get(table_name, "nothing")
    # Execute the function
    return func


def get_indices(table_name):
    switcher = {
        "customer": [0, 3, 5],
        "region": [0],
        "nation": [0, 2],
        "lineitem": [0, 1, 2, 3, 4, 5, 6, 7],
        "orders": [0, 1, 3],
        "supplier": [0, 3, 5],
        "part": [0, 5, 7],
        "partsupp": [0, 1, 2, 3],
    }
    # Get the function from switcher dictionary
    func = switcher.get(table_name, "nothing")
    # Execute the function
    return func


def get_dtypes_wstrings(table_name):
    switcher = {
        "customer": [
            "int32",
            "str",
            "str",
            "int32",
            "str",
            "float64",
            "str",
            "str"
        ],
        "region": ["int32", "str", "str"],
        "nation": ["int32", "str", "int32", "str"],
        "lineitem": [
            "int64",
            "int64",
            "int64",
            "int32",
            "float64",
            "float64",
            "float64",
            "float64",
            "str",
            "str",
            "date64",
            "date64",
            "date64",
            "str",
            "str",
            "str",
        ],
        "orders": [
            "int64",
            "int32",
            "str",
            "float64",
            "date64",
            "str",
            "str",
            "str",
            "str",
        ],
        "supplier": ["int64", "str", "str", "int32", "str", "float64", "str"],
        "part": [
            "int64",
            "str",
            "str",
            "str",
            "str",
            "int64",
            "str",
            "float32",
            "str"
        ],
        "partsupp": ["int64", "int64", "int64", "float32", "str"],
    }
    # Get the function from switcher dictionary
    func = switcher.get(table_name, "nothing")
    # Execute the function
    return func


def get_dtypes(table_name, bool_column=False):
    switcher = {
        "customer": [
            "int32",
            "str",
            "str",
            "int32",
            "str",
            "float64",
            "str",
            "str"
        ],
        "region": ["int32", "str", "str"],
        "nation": ["int32", "str", "int32", "str"],
        "lineitem": [
            "int64",
            "int64",
            "int64",
            "int32",
            "float64",
            "float64",
            "float64",
            "float64",
            "str",
            "str",
            "date64",
            "date64",
            "date64",
            "str",
            "str",
            "str",
        ],
        "orders": [
            "int64",
            "int32",
            "str",
            "float64",
            "date64",
            "str",
            "str",
            "int32",
            "str",
        ],
        "supplier": ["int64", "str", "str", "int32", "str", "float64", "str"],
        "part": [
            "int64",
            "str",
            "str",
            "str",
            "str",
            "int64",
            "str",
            "float32",
            "str"
        ],
        "partsupp": ["int64", "int64", "int64", "float32", "str"],
    }

    if bool_column:
        switcher.update(
            {
                "bool_orders": [
                    "int64",
                    "int32",
                    "str",
                    "float64",
                    "date64",
                    "str",
                    "str",
                    "str",
                    "str",
                    "boolean",
                ]
            }
        )

    # Get the function from switcher dictionary
    func = switcher.get(table_name, "nothing")
    # Execute the function
    return func

def get_dtypes_pandas(table_name):
    switcher = {
        "customer": {
            "c_custkey": "int64",
            "c_name": "str",
            "c_address": "str",
            "c_nationkey": "int64",
            "c_phone": "str",
            "c_acctbal": "float64",
            "c_mktsegment": "str",
            "c_comment": "str",
        },
        "region": {
            "r_regionkey": "int64",
            "r_name": "str",
            "r_comment": "str"
        },
        "nation": {
            "n_nationkey": "int64",
            "n_name": "str",
            "n_regionkey": "int64",
            "n_comment": "str",
        },
        "lineitem": {
            "l_orderkey": "int64",
            "l_partkey": "int64",
            "l_suppkey": "int64",
            "l_linenumber": "int64",
            "l_quantity": "float64",
            "l_extendedprice": "float64",
            "l_discount": "float64",
            "l_tax": "float64",
            "l_returnflag": "str",
            "l_linestatus": "str",
            "l_shipdatetime64": "datetime64",
            "l_commitdatetime64": "datetime64",
            "l_receiptdatetime64": "datetime64",
            "l_shipinstruct": "str",
            "l_shipmode": "str",
            "l_comment": "str",
        },
        "orders": {
            "o_orderkey": "int64",
            "o_custkey": "int64",
            "o_orderstatus": "str",
            "o_totalprice": "float64",
            "o_orderdatetime64": "datetime64",
            "o_orderpriority": "str",
            "o_clerk": "str",
            "o_shippriority": "int64",
            "o_comment": "str",
        },
        "supplier": {
            "s_suppkey": "int64",
            "s_name": "str",
            "s_address": "str",
            "s_nationkey": "int64",
            "s_phone": "str",
            "s_acctbal": "float64",
            "s_comment": "str",
        },
        "part": {
            "p_partkey": "int64",
            "p_name": "str",
            "p_mfgr": "str",
            "p_brand": "str",
            "p_type": "str",
            "p_size": "int64",
            "p_container": "str",
            "p_retailprice": "float64",
            "p_comment": "str",
        },
        "partsupp": {
            "ps_partkey": "int64",
            "ps_suppkey": "int64",
            "ps_availqty": "int64",
            "ps_supplycost": "float64",
            "ps_comment": "str",
        },
    }

    # Get the function from switcher dictionary
    func = switcher.get(table_name, "nothing")
    # Execute the function
    return func


def get_dtypes_spark(type):
    switcher = {
        "int32": st.IntegerType(),
        "int64": st.LongType(),
        "float32": st.FloatType(),
        "float64": st.DoubleType(),
        "date64": st.DateType(),  # TimestampType
        "str": st.StringType(),
        "boolean": st.BooleanType(),
    }

    func = switcher.get(type, "nothing")
    # Execute the function
    return func


def get_dtypes_hive(type):
    switcher = {
        "int32": "int",
        "int64": "bigint",
        "float32": "float",
        "float64": "double",
        "date64": "timestamp",  # TimestampType
        "str": "string",
        "boolean": "boolean",
    }

    func = switcher.get(type, "nothing")
    # Execute the function
    return func


def Read_tpch_files(column_names, files_dir, table, data_types):

    table_pdf = None
    dataframes = []

    for dataFile in listdir(files_dir):
        if isdir(files_dir + "/" + dataFile):
            continue
        dataFileTokens = dataFile.split(".")
        dataFileName = dataFileTokens[0]
        dataFileExt = dataFileTokens[1]
        tableName_ = table + "_"
        if dataFileExt == "psv":
            if (table == dataFileName) or re.match(tableName_, dataFileName):
                file_dir = files_dir + "/" + dataFile
                tmp = cudf.read_csv(
                    file_dir, delimiter="|", names=column_names,
                    dtype=data_types
                )
                dataframes.append(tmp)
    if len(dataframes) != 0:
        table_pdf = cudf.concat(dataframes)
    return table_pdf


def get_filenames_for_table(table, dir_data_lc, ext, dir_data_fs):
    dataFiles = []
    for dataFile in listdir(dir_data_lc):
        if isdir(dir_data_lc + "/" + dataFile):
            continue
        dataFileTokens = dataFile.split(".")
        dataFileName = dataFileTokens[0]
        dataFileExt = dataFileTokens[1]
        tableName_ = table + "_"
        if dataFileExt == ext:
            if (table == dataFileName) or re.match(tableName_, dataFileName):
                if dir_data_fs == "":
                    file_dir = dir_data_lc + "/" + dataFile
                else:
                    file_dir = dir_data_fs + dataFile
                dataFiles.append(file_dir)
    return dataFiles


def get_filenames_table(table, dir_data_lc, n_files, ext='psv', **kwargs):
    full_path = kwargs.get("full_path")
    if full_path is None:
        full_path = True
    dataFiles = []
    c = 0
    while c < n_files:
        for dataFile in listdir(dir_data_lc):
            dataFileTokens = dataFile.split(".")
            dataFileName = dataFileTokens[0]
            dataFileExt = dataFileTokens[1]
            tableName_ = table + "_"
            if dataFileExt == ext:
                if table == dataFileName or re.match(tableName_, dataFileName):
                    if c < n_files:
                        if full_path:
                            dataFiles.append(dir_data_lc + "/" + dataFile)
                        else:
                            dataFiles.append(dataFile)
                        c = c + 1
                    else:
                        break

    return dataFiles


def read_data(table, dir_data_file, bool_column=False):
    column_names = get_column_names(table, bool_column)
    data_types = get_dtypes(table, bool_column)
    table_gdf = Read_tpch_files(column_names, dir_data_file, table, data_types)

    return table_gdf


def read_data_pandas(table, file_dirs):
    column_names = get_column_names(table)
    table_pdf = None
    count = 1
    print(get_dtypes_pandas(table))
    for dataFile in listdir(file_dirs):
        if isdir(file_dirs + "/" + dataFile):
            continue
        dataFileTokens = dataFile.split(".")
        dataFileName = dataFileTokens[0]
        dataFileExt = dataFileTokens[1]
        tableName_ = table + "_"
        if dataFileExt == "psv":
            if (table == dataFileName) or re.match(tableName_, dataFileName):
                file_dir = file_dirs + "/" + dataFile
                if count == 1:
                    table_pdf = pd.read_csv(
                        file_dir,
                        delimiter="|",
                        names=column_names,
                        dtype=get_dtypes_pandas(table),
                        parse_dates=True,
                    )
                else:
                    tmp = pd.read_csv(
                        file_dir,
                        delimiter="|",
                        names=column_names,
                        dtype=get_dtypes_pandas(table),
                        parse_dates=True,
                    )
                    table_pdf = table_pdf.append(tmp)

                count = count + 1

    return table_pdf


def read_data_pandas_parquet(table, file_dirs):
    table_pdf = None
    count = 1

    for dataFile in listdir(file_dirs):
        dataFileTokens = dataFile.split(".")
        dataFileName = dataFileTokens[0]
        dataFileExt = dataFileTokens[1]
        tableName_ = table + "_"
        if dataFileExt == "parquet":
            if (table == dataFileName) or re.match(tableName_, dataFileName):
                file_dir = file_dirs + "/" + dataFile
                if count == 1:
                    table_pdf = pd.read_parquet(file_dir, engine="pyarrow")
                else:
                    tmp = pd.read_parquet(file_dir, engine="pyarrow")
                    table_pdf = table_pdf.append(tmp)

                count = count + 1

    return table_pdf


# ************************* READ MORTGAGE DATA *****************************


def get_type_schema(path):
    format = path.split(".")[-1]

    if format == "parquet":
        return "SchemaFrom.ParquetFile"
    elif format == "csv" or format == "psv" or format.startswith("txt"):
        return "SchemaFrom.loadFile"


def gpu_load_performance_csv(performance_path, **kwargs):
    """ Loads performance data

    Returns
    -------
    GPU DataFrame
    """
    chronometer = Chronometer.makeStarted()

    cols = [
        "loan_id",
        "monthly_reporting_period",
        "servicer",
        "interest_rate",
        "current_actual_upb",
        "loan_age",
        "remaining_months_to_legal_maturity",
        "adj_remaining_months_to_maturity",
        "maturity_date",
        "msa",
        "current_loan_delinquency_status",
        "mod_flag",
        "zero_balance_code",
        "zero_balance_effective_date",
        "last_paid_installment_date",
        "foreclosed_after",
        "disposition_date",
        "foreclosure_costs",
        "prop_preservation_and_repair_costs",
        "asset_recovery_costs",
        "misc_holding_expenses",
        "holding_taxes",
        "net_sale_proceeds",
        "credit_enhancement_proceeds",
        "repurchase_make_whole_proceeds",
        "other_foreclosure_proceeds",
        "non_interest_bearing_upb",
        "principal_forgiveness_upb",
        "repurchase_make_whole_proceeds_flag",
        "foreclosure_principal_write_off_amount",
        "servicing_activity_indicator",
    ]

    dtypes = OrderedDict(
        [
            ("loan_id", "int64"),
            ("monthly_reporting_period", "date"),
            ("servicer", "category"),
            ("interest_rate", "float64"),
            ("current_actual_upb", "float64"),
            ("loan_age", "float64"),
            ("remaining_months_to_legal_maturity", "float64"),
            ("adj_remaining_months_to_maturity", "float64"),
            ("maturity_date", "date"),
            ("msa", "float64"),
            ("current_loan_delinquency_status", "int32"),
            ("mod_flag", "category"),
            ("zero_balance_code", "category"),
            ("zero_balance_effective_date", "date"),
            ("last_paid_installment_date", "date"),
            ("foreclosed_after", "date"),
            ("disposition_date", "date"),
            ("foreclosure_costs", "float64"),
            ("prop_preservation_and_repair_costs", "float64"),
            ("asset_recovery_costs", "float64"),
            ("misc_holding_expenses", "float64"),
            ("holding_taxes", "float64"),
            ("net_sale_proceeds", "float64"),
            ("credit_enhancement_proceeds", "float64"),
            ("repurchase_make_whole_proceeds", "float64"),
            ("other_foreclosure_proceeds", "float64"),
            ("non_interest_bearing_upb", "float64"),
            ("principal_forgiveness_upb", "float64"),
            ("repurchase_make_whole_proceeds_flag", "category"),
            ("foreclosure_principal_write_off_amount", "float64"),
            ("servicing_activity_indicator", "category"),
        ]
    )
    print(performance_path)
    performance_table = pyblazing.create_table(
        table_name="perf",
        type=get_type_schema(performance_path),
        path=performance_path,
        delimiter="|",
        names=cols,
        dtypes=dtypes,  # TODO: dtypes=get_dtype_values(dtypes)
        skip_rows=1,
    )
    Chronometer.show(chronometer, "Read Performance CSV")
    return performance_table


def gpu_load_acquisition_csv(acquisition_path, **kwargs):
    """ Loads acquisition data

    Returns
    -------
    GPU DataFrame
    """
    chronometer = Chronometer.makeStarted()

    cols = [
        "loan_id",
        "orig_channel",
        "seller_name",
        "orig_interest_rate",
        "orig_upb",
        "orig_loan_term",
        "orig_date",
        "first_pay_date",
        "orig_ltv",
        "orig_cltv",
        "num_borrowers",
        "dti",
        "borrower_credit_score",
        "first_home_buyer",
        "loan_purpose",
        "property_type",
        "num_units",
        "occupancy_status",
        "property_state",
        "zip",
        "mortgage_insurance_percent",
        "product_type",
        "coborrow_credit_score",
        "mortgage_insurance_type",
        "relocation_mortgage_indicator",
    ]

    dtypes = OrderedDict(
        [
            ("loan_id", "int64"),
            ("orig_channel", "category"),
            ("seller_name", "category"),
            ("orig_interest_rate", "float64"),
            ("orig_upb", "int64"),
            ("orig_loan_term", "int64"),
            ("orig_date", "date"),
            ("first_pay_date", "date"),
            ("orig_ltv", "float64"),
            ("orig_cltv", "float64"),
            ("num_borrowers", "float64"),
            ("dti", "float64"),
            ("borrower_credit_score", "float64"),
            ("first_home_buyer", "category"),
            ("loan_purpose", "category"),
            ("property_type", "category"),
            ("num_units", "int64"),
            ("occupancy_status", "category"),
            ("property_state", "category"),
            ("zip", "int64"),
            ("mortgage_insurance_percent", "float64"),
            ("product_type", "category"),
            ("coborrow_credit_score", "float64"),
            ("mortgage_insurance_type", "float64"),
            ("relocation_mortgage_indicator", "category"),
        ]
    )

    print(acquisition_path)

    acquisition_table = pyblazing.create_table(
        table_name="acq",
        type=get_type_schema(acquisition_path),
        path=acquisition_path,
        delimiter="|",
        names=cols,
        dtypes=dtypes,  # TODO: dtypes=get_dtype_values(dtypes)
        skip_rows=1,
    )
    Chronometer.show(chronometer, "Read Acquisition CSV")
    return acquisition_table


def gpu_load_names(col_names_path, **kwargs):
    """ Loads names used for renaming the banks

    Returns
    -------
    GPU DataFrame
    """
    chronometer = Chronometer.makeStarted()

    cols = ["seller_name", "new_seller_name"]

    dtypes = OrderedDict(
        [("seller_name", "category"), ("new_seller_name", "category"), ]
    )
    new = col_names_path + "names.load"
    print(new)
    names_table = pyblazing.create_table(
        table_name="names",
        type=get_type_schema(new),
        path=new,
        delimiter="|",
        names=cols,
        dtypes=dtypes,  # TODO: dtypes=get_dtype_values(dtypes)
        skip_rows=1,
    )
    Chronometer.show(chronometer, "Read Names CSV")
    return names_table


# NOTE fileSchemaType is pyblazing.FileSchemaType
# (see /blazingdb-protocol/python/blazingdb/messages/blazingdb/
#                                               protocol/DataType.py)
def get_extension(fileSchemaType):
    switcher = {
        DataType.CUDF: "gdf",
        DataType.CSV: "psv",
        DataType.PARQUET: "parquet",
        DataType.JSON: "json",
        DataType.ORC: "orc",
        DataType.DASK_CUDF: "dask_cudf",
        DataType.MYSQL: "mysql",
        DataType.POSTGRESQL: "postgresql",
        DataType.SQLITE: "sqlite",
    }
    return switcher.get(fileSchemaType)


# NOTE 'bool_orders_index' is the index where the table bool_orders
# inside 'tables'
def create_tables(bc, dir_data_lc, fileSchemaType, **kwargs):
    ext = get_extension(fileSchemaType)

    tables = kwargs.get("tables", tpchTables)
    table_names = kwargs.get("table_names", tables)
    bool_orders_index = kwargs.get("bool_orders_index", -1)

    testsWithNulls = Settings.data["RunSettings"]["testsWithNulls"]

    if tables[0] in smilesTables:
        dir_data_lc = dir_data_lc + "smiles/"
    else:
        if testsWithNulls == "true":
            dir_data_lc = dir_data_lc + "tpch-with-nulls/"
        else:
            dir_data_lc = dir_data_lc + "tpch/"

    for i, table in enumerate(tables):
        # using wildcard, note the _ after the table name
        # (it will avoid collisions)
        table_files = ("%s/%s_[0-9]*.%s") % (dir_data_lc, table, ext)
        if fileSchemaType == DataType.CSV or fileSchemaType == DataType.JSON:
            bool_orders_flag = False

            if i == bool_orders_index:
                bool_orders_flag = True

            dtypes = get_dtypes(table, bool_orders_flag)
            col_names = get_column_names(table, bool_orders_flag)
            bc.create_table(
                table_names[i], table_files, delimiter="|", dtype=dtypes,
                names=col_names
            )
        elif fileSchemaType == DataType.CUDF:
            bool_column = bool_orders_index != -1
            gdf = read_data(table, dir_data_lc, bool_column)
            bc.create_table(table_names[i], gdf)
        elif fileSchemaType == DataType.DASK_CUDF:
            nRals = Settings.data["RunSettings"]["nRals"]
            num_partitions = nRals
            bool_column = bool_orders_index != -1
            gdf = read_data(table, dir_data_lc, bool_column)
            ds = dask_cudf.from_cudf(gdf, npartitions=num_partitions)
            bc.create_table(table_names[i], ds)
        # elif fileSchemaType == DataType.DASK_CUDF:
        #     bool_column = bool_orders_index != -1
        #     table_files = ("%s/%s_[0-9]*.%s") % (dir_data_lc, table,
        #                                                       'parquet')
        #     dask_df = dask_cudf.read_parquet(table_files)
        #     dask_df = bc.unify_partitions(dask_df)
        #     t = bc.create_table(table, dask_df)
        elif fileSchemaType in [DataType.MYSQL, DataType.POSTGRESQL, DataType.SQLITE]:
            sql_table_filter_map = kwargs.get("sql_table_filter_map", {})
            sql_table_batch_size_map = kwargs.get("sql_table_batch_size_map", {})
            sql = kwargs.get("sql_connection", None)

            from_sql = SQLEngineStringDataTypeMap[fileSchemaType]
            sql_hostname = sql.hostname
            sql_port = sql.port
            sql_username = sql.username
            sql_password = sql.password
            sql_schema = sql.schema
            sql_table_filter = ""
            sql_table_batch_size = 1000

            if table in sql_table_filter_map:
                sql_table_filter = sql_table_filter_map[table]
            if table in sql_table_batch_size_map:
                sql_table_batch_size = sql_table_batch_size_map[table]

            bc.create_table(table_names[i], table,
                from_sql = from_sql,
                hostname = sql_hostname,
                port = sql_port,
                username = sql_username,
                password = sql_password,
                schema = sql_schema,
                table_filter = sql_table_filter,
                table_batch_size = sql_table_batch_size)
        else:
            bc.create_table(table_names[i], table_files)

        # TODO percy kharoly bindings
        # if (not t.is_valid()):
        #    raise RuntimeError("Could not create the table " + table +
        # " using " + str(table_files))


def create_hive_tables(bc, dir_data_lc, fileSchemaType, **kwargs):
    tables = kwargs.get("tables", tpchTables)
    for i, table in enumerate(tables):
        cursor = hive.connect("172.22.0.3").cursor()
        table = bc.create_table(table, cursor)
        # table = bc.create_table(table, cursor, file_format=fileSchemaType)
        print(table)

@unique
class HiveCreateTableType(IntEnum):
    AUTO = 0,
    WITH_PARTITIONS = 1

def create_hive_partitions_tables(bc, dir_partitions, fileSchemaType, createTableType, partitions,
                                  partitions_schema, **kwargs):
    ext = get_extension(fileSchemaType)

    if fileSchemaType not in [DataType.CSV, DataType.PARQUET, DataType.ORC]:
        raise RuntimeError("It is not a valid file format for create table hive")

    tables = kwargs.get("tables", tpchTables)

    if createTableType == HiveCreateTableType.AUTO:
        for i, table in enumerate(tables):
            if fileSchemaType == DataType.CSV:
                dtypes = get_dtypes(table)
                col_names = get_column_names(table)
                bc.create_table(table, dir_partitions + table, file_format=ext, delimiter="|", dtype=dtypes,
                                names=col_names)

            else:
                bc.create_table(table, dir_partitions + table, file_format=ext)

    elif createTableType == HiveCreateTableType.WITH_PARTITIONS:
        for i, table in enumerate(tables):
            if fileSchemaType == DataType.CSV:
                dtypes = get_dtypes(table)
                col_names = get_column_names(table)
                bc.create_table(table,
                                dir_partitions + table,
                                file_format=ext,
                                partitions=partitions,
                                partitions_schema=partitions_schema,
                                delimiter="|",
                                dtype=dtypes,
                                names=col_names)

            else:
                bc.create_table(table,
                                dir_partitions + table,
                                file_format=ext,
                                partitions=partitions,
                                partitions_schema=partitions_schema)
