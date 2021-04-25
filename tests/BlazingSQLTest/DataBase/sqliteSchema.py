import csv
import glob
import os
import sqlite3

from tempfile import NamedTemporaryFile

from .createSchema import sql_connection


sqlite_tpch_table_descriptions = {
    'nation': '''create table nation  ( n_nationkey  integer,
                                n_name       char(25),
                                n_regionkey  integer,
                                n_comment    varchar(152))''',
    'region': '''create table region  ( r_regionkey  integer,
                            r_name       char(25),
                            r_comment    varchar(152))''',
    'part': '''create table part  ( p_partkey     integer ,
                          p_name        varchar(55) ,
                          p_mfgr        char(25) ,
                          p_brand       char(10) ,
                          p_type        varchar(25) ,
                          p_size        integer ,
                          p_container   char(10) ,
                          p_retailprice decimal(15,2) ,
                          p_comment     varchar(23) )''',
    'supplier': '''create table supplier ( s_suppkey     integer ,
                             s_name        char(25) ,
                             s_address     varchar(40) ,
                             s_nationkey   integer ,
                             s_phone       char(15) ,
                             s_acctbal     decimal(15,2) ,
                             s_comment     varchar(101) )''',
    'partsupp': '''create table partsupp ( ps_partkey     integer ,
                             ps_suppkey     integer ,
                             ps_availqty    integer ,
                             ps_supplycost  decimal(15,2)  ,
                             ps_comment     varchar(199)  );''',
    'customer': '''create table customer ( c_custkey     integer ,
                             c_name        varchar(25) ,
                             c_address     varchar(40) ,
                             c_nationkey   integer ,
                             c_phone       char(15) ,
                             c_acctbal     decimal(15,2)   ,
                             c_mktsegment  char(10) ,
                             c_comment     varchar(117) );''',
    'orders': '''create table orders  ( o_orderkey       integer ,
                           o_custkey        integer ,
                           o_orderstatus    char(1) ,
                           o_totalprice     decimal(15,2) ,
                           o_orderdate      date ,
                           o_orderpriority  char(15) ,
                           o_clerk          char(15) ,
                           o_shippriority   integer ,
                           o_comment        varchar(79) )''',
    'lineitem': '''create table lineitem ( l_orderkey    integer ,
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
                             l_comment      varchar(44) )''',
}


def create_sqlite_table(table_description: str,
                        cursor: sqlite3.Cursor) -> bool:
    try:
        print(f'Creating table {table_description}', end='')
        cursor.execute(table_description)
    except sqlite3.OperationalError as error:
        if 'already exists' in str(error):
            print('already exists.')
            return False
        else:
            raise ValueError(
                f'Error creating from\n{table_description}') from error
    else:
        print('OK')
    return True


def sqlite_load_data_in_file(table: str,
                             full_path_wildcard: str,
                             cursor: sqlite3.Cursor,
                             connection: sqlite3.Connection):
    psvpaths = glob.glob(full_path_wildcard)
    for psvpath in psvpaths:
        with open(psvpath) as psv:
            reader = csv.reader(psv, delimiter='|')
            row = next(reader)
            nfields = ','.join('?' * len(row))
            query = f'insert into {table} values ({nfields})'
            cursor.execute(query, row)
            for row in reader:
                cursor.execute(query, row)
            connection.commit()


def create_and_load_tpch_schema(sql: sql_connection,
                                only_create_tables: bool = False):
    schema = sql.schema
    if not schema:
        temporaryFile = NamedTemporaryFile(delete=False)
        schema = temporaryFile.name
    connection = sqlite3.connect(schema)

    cursor = connection.cursor()

    conda_prefix = os.environ.get('CONDA_PREFIX', '')
    tabs_dir = os.path.join(
        conda_prefix,
        'blazingsql-testing-files/data/tpch-with-nulls/')

    for table, table_description in sqlite_tpch_table_descriptions.items():
        ok = create_sqlite_table(table_description, cursor)
        if ok and not only_create_tables:
            table_files = '%s/%s_*.psv' % (tabs_dir, table)
            sqlite_load_data_in_file(table, table_files, cursor, connection)
        else:
            print(
                'SQLite table %s already exists, will not load any data!' %
                table)

    cursor.close()
    connection.close()
