import os
import glob

from .createSchema import get_sql_connection, get_column_names, sql_connection

from blazingsql import DataType

import psycopg2


postgresql_tpch_table_descriptions = {
    "nation": """create table nation  ( n_nationkey  integer,
                                n_name       char(25),
                                n_regionkey  integer,
                                n_comment    varchar(152))""",
    "region": """create table region  ( r_regionkey  integer,
                            r_name       char(25),
                            r_comment    varchar(152))""",
    "part": """create table part  ( p_partkey     integer ,
                          p_name        varchar(55) ,
                          p_mfgr        char(25) ,
                          p_brand       char(10) ,
                          p_type        varchar(25) ,
                          p_size        integer ,
                          p_container   char(10) ,
                          p_retailprice decimal(15,2) ,
                          p_comment     varchar(23) )""",
    "supplier": """create table supplier ( s_suppkey     integer ,
                             s_name        char(25) ,
                             s_address     varchar(40) ,
                             s_nationkey   integer ,
                             s_phone       char(15) ,
                             s_acctbal     decimal(15,2) ,
                             s_comment     varchar(101) )""",
    "partsupp": """create table partsupp ( ps_partkey     integer ,
                             ps_suppkey     integer ,
                             ps_availqty    integer ,
                             ps_supplycost  decimal(15,2)  ,
                             ps_comment     varchar(199)  );""",
    "customer": """create table customer ( c_custkey     integer ,
                             c_name        varchar(25) ,
                             c_address     varchar(40) ,
                             c_nationkey   integer ,
                             c_phone       char(15) ,
                             c_acctbal     decimal(15,2)   ,
                             c_mktsegment  char(10) ,
                             c_comment     varchar(117) );""",
    "orders": """create table orders  ( o_orderkey       integer ,
                           o_custkey        integer ,
                           o_orderstatus    char(1) ,
                           o_totalprice     decimal(15,2) ,
                           o_orderdate      date ,
                           o_orderpriority  char(15) ,  
                           o_clerk          char(15) , 
                           o_shippriority   integer ,
                           o_comment        varchar(79) )""",
    "lineitem": """create table lineitem ( l_orderkey    integer ,
                             l_partkey     integer ,
                             l_suppkey     integer ,
                             l_linenumber  integer ,
                             l_quantity    decimal(15,2) ,
                             l_extendedprice  decimal(15,2) ,
                             l_discount    decimal(15,2) ,
                             l_tax         decimal(15,2) ,
                             l_returnflag  char(1) ,
                             l_linestatus  char(1) ,
                             l_shipdate    date ,
                             l_commitdate  date ,
                             l_receiptdate date ,
                             l_shipinstruct char(25) ,
                             l_shipmode     char(10) ,
                             l_comment      varchar(44) )""",
}


# if table already exists returns False
def create_postgresql_table(table_description: str, cursor) -> bool:
    print("Creating table {}: ".format(table_description), end='')
    cursor.execute(table_description)
    return True


def copy(cursor, csvFile, tableName):
    csvDelimiter = "'|'"
    csvQuoteCharacter = "'\"'"
    query = "COPY %s FROM STDIN WITH CSV QUOTE %s DELIMITER AS %s NULL as 'null'" % (tableName, csvQuoteCharacter, csvDelimiter)
    cursor.copy_expert(sql = query, file = csvFile)


def postgresql_load_data_in_file(table: str, full_path_wildcard: str, cursor, cnx):
    cols = get_column_names(table)
    h = ""
    b = ""
    for i,c in enumerate(cols):
        h = h + "@" + c
        hj = "%s = NULLIF(@%s,'null')" % (c,c)
        b = b + hj
        if i + 1 != len(cols):
            h = h + ",\n"
            b = b + ",\n"

    a = glob.glob(full_path_wildcard)
    for fi in a:
        with open(fi, 'r') as csvFile:
            copy(cursor, csvFile, table)
            cnx.commit()
    print("load data done!")


# using the nulls dataset
def create_and_load_tpch_schema(sql: sql_connection, only_create_tables : bool = False):
    #allow_local_infile = True)
    cnx = psycopg2.connect(
        dbname=sql.schema,
        user=sql.username,
        host=sql.hostname,
        port=int(sql.port),
        password=sql.password
    )
    cursor = cnx.cursor()

    conda_prefix = os.getenv("CONDA_PREFIX", "")
    tabs_dir = conda_prefix + "/" + "blazingsql-testing-files/data/tpch-with-nulls/"

    for table, table_description in postgresql_tpch_table_descriptions.items():
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
        cnx.commit()
        ok = create_postgresql_table(table_description, cursor)
        cnx.commit()
        if ok and not only_create_tables:
            table_files = "%s/%s_*.psv" % (tabs_dir, table)
            postgresql_load_data_in_file(table, table_files, cursor, cnx)
        else:
            print("MySQL table %s already exists, will not load any data!" % table)

    cursor.close()
    cnx.close()
