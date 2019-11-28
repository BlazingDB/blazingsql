from collections import OrderedDict

tables = OrderedDict([
    ('customer', OrderedDict([
        ('c_custkey', 'int'),
        ('c_name', 'string(32)'),
        ('c_address', 'string(128)'),
        ('c_nationkey', 'short'),
        ('c_phone', 'string(16)'),
        ('c_acctbal', 'float'),
        ('c_mktsegment', 'string(16)'),
        ('c_comment', 'string(120)')
    ])),
    ('lineitem', OrderedDict([
        ('l_orderkey', 'long'),
        ('l_partkey', 'int'),
        ('l_suppkey', 'int'),
        ('l_linenumber', 'short'),
        ('l_quantity', 'float'),
        ('l_extendedprice', 'float'),
        ('l_discount', 'float'),
        ('l_tax', 'float'),
        ('l_returnflag', 'string(8)'),
        ('l_linestatus', 'string(8)'),
        ('l_shipdate', 'date'),
        ('l_commitdate', 'date'),
        ('l_receiptdate', 'date'),
        ('l_shipinstruct', 'string(32)'),
        ('l_shipmode', 'string(16)'),
        ('l_comment', 'string(48)')
    ])),
    ('nation', OrderedDict([
        ('n_nationkey', 'short'),
        ('n_name', 'string(32)'),
        ('n_regionkey', 'short'),
        ('n_comment', 'string(152)')
    ])),
    ('orders', OrderedDict([
        ('o_orderkey', 'long'),
        ('o_custkey', 'int'),
        ('o_orderstatus', 'string(8)'),
        ('o_totalprice', 'float'),
        ('o_orderdate', 'date'),
        ('o_orderpriority', 'string(16)'),
        ('o_clerk', 'string(16)'),
        ('o_shippriority', 'string(8)'),
        ('o_comment', 'string(80)'),
    ])),
    ('part', OrderedDict([
        ('p_partkey', 'int'),
        ('p_name', 'string(56)'),
        ('p_mfgr', 'string(32)'),
        ('p_brand', 'string(16)'),
        ('p_type', 'string(32)'),
        ('p_size', 'short'),
        ('p_container', 'string(16)'),
        ('p_retailprice', 'float'),
        ('p_comment', 'string(24)')
    ])),
    ('partsupp', OrderedDict([
        ('ps_partkey', 'int'),
        ('ps_suppkey', 'int'),
        ('ps_availqty', 'short'),
        ('ps_supplycost', 'float'),
        ('ps_comment', 'string(200)')
    ])),
    ('region', OrderedDict([
        ('r_regionkey', 'long'),
        ('r_name', 'string(32)'),
        ('r_comment', 'string(152)')
    ])),
    ('supplier', OrderedDict([
        ('s_suppkey', 'int'),
        ('s_name', 'string(32)'),
        ('s_address', 'string(40)'),
        ('s_nationkey', 'long'),
        ('s_phone', 'string(16)'),
        ('s_acctbal', 'double'),
        ('s_comment', 'string(104)')
    ]))
])

tables['customer']['c_acctbal'] = 'double'
tables['lineitem']['l_quantity'] = 'double'
tables['lineitem']['l_extendedprice'] = 'double'
tables['lineitem']['l_discount'] = 'double'
tables['lineitem']['l_tax'] = 'double'
tables['orders']['o_totalprice'] = 'double'
tables['orders']['o_shippriority'] = 'short'
tables['part']['p_retailprice'] = 'double'
tables['partsupp']['ps_supplycost'] = 'double'
tables['region']['r_regionkey'] = 'short'
tables['supplier']['s_nationkey'] = 'short'

tableNames = [tableName for tableName, tableColumns in tables.items()]

import os
def init_schema(drill, tpch_dir):
  dir_path = os.path.dirname(os.path.realpath(__file__))
  print(dir_path)

  for name in tableNames:
    drill.query('DROP TABLE IF EXISTS dfs.tmp.`%(table)s`' % {'table': name})

  drill.query('''
  create table dfs.tmp.`orders/` as
    select cast(columns[0] as bigint) as o_orderkey,
          cast(columns[1] as int) as o_custkey,
          columns[2] as o_orderstatus,
          cast(columns[3] as double) as o_totalprice,
          columns[4] as o_orderdate,
          columns[5] as o_orderpriority,
          columns[6] as o_clerk,
          cast(columns[7] as int) as o_shippriority,
          columns[8] as o_comment
  FROM table(dfs.`%(tpch_dir)s/orders.psv`(type => 'text', fieldDelimiter => '|'))
  ''' % {'tpch_dir': tpch_dir} )

  drill.query('''
  create table dfs.tmp.`lineitem/` as
  select cast(columns[0] as bigint) as l_orderkey,
         cast(columns[1] as int) as l_partkey,
         cast(columns[2] as int) as l_suppkey,
         cast(columns[3] as int) as l_linenumber,
         cast(columns[4] as double) as l_quantity,
         cast(columns[5] as double) as l_extendedprice,
         cast(columns[6] as double) as l_discount,
         cast(columns[7] as double) as l_tax,
         columns[8] as l_returnflag,
         columns[9] as l_linestatus,
         columns[10] as l_shipdate,
         columns[11] as l_commitdate,
         columns[12] as l_receiptdate,
         columns[13] as l_shipinstruct,
         columns[14] as l_shipmode,
         columns[15] as l_comment
FROM table(dfs.`%(tpch_dir)s/lineitem.psv`(type => 'text', fieldDelimiter => '|'))
  ''' % {'tpch_dir': tpch_dir} )


  drill.query('''
  create table dfs.tmp.`nation/` as
    select cast(columns[0] as int) as n_nationkey,
          columns[1] as n_name,
          cast(columns[2] as int) as n_regionkey,
          columns[3] as n_comment
  FROM table(dfs.`%(tpch_dir)s/nation.psv`(type => 'text', fieldDelimiter => '|'))
  ''' % {'tpch_dir': tpch_dir} )

  drill.query('''
  create table dfs.tmp.`customer` as
          select cast(columns[0] as int) as c_custkey,
          columns[1] as c_name,
          columns[2] as c_address,
          cast(columns[3] as int) as c_nationkey,
          columns[4] as c_phone,
          cast(columns[5] as double) as c_acctbal,
          columns[6] as c_mktsegment,
          columns[7] as c_comment
  FROM table(dfs.`%(tpch_dir)s/customer.psv`(type => 'text', fieldDelimiter => '|'))
  ''' % {'tpch_dir': tpch_dir} )

  drill.query('''
  create table dfs.tmp.`part/` as
  select cast(columns[0] as int) as p_partkey,
          columns[1] as p_name,
          columns[2] as p_mfgr,
          columns[3] as p_brand,
          columns[4] as p_type,
          cast(columns[5] as int) as p_size,
          columns[6] as p_container,
          cast(columns[7] as double) as p_retailprice,
          columns[8] as p_comment
  FROM table(dfs.`%(tpch_dir)s/part.psv`(type => 'text', fieldDelimiter => '|'))
  ''' % {'tpch_dir': tpch_dir} )

  drill.query('''
  create table dfs.tmp.`partsupp/` as
    select cast(columns[0] as int) as ps_partkey,
          cast(columns[1] as int) as ps_suppkey,
          cast(columns[2] as int) as ps_availqty,
          cast(columns[3] as double) as ps_supplycost,
          columns[4] as ps_comment
  FROM table(dfs.`%(tpch_dir)s/partsupp.psv`(type => 'text', fieldDelimiter => '|'))
  ''' % {'tpch_dir': tpch_dir} )



  drill.query('''
  create table dfs.tmp.`supplier/` as
  select cast(columns[0] as int) as s_suppkey,
          columns[1] as s_name,
          columns[2] as s_address,
          cast(columns[3] as int) as s_nationkey,
          columns[4] as s_phone,
          cast(columns[5] as double) as s_acctbal,
          columns[6] as s_comment
  FROM table(dfs.`%(tpch_dir)s/supplier.psv`(type => 'text', fieldDelimiter => '|'))
    ''' % {'tpch_dir': tpch_dir} )


  drill.query('''
  CREATE TABLE dfs.tmp.`region/` AS
  SELECT cast(columns[0] as int) as `r_regionkey`,
          columns[1] as `r_name`,
          columns[2] as `r_comment`
  FROM table(dfs.`%(tpch_dir)s/region.psv`(type => 'text', fieldDelimiter => '|'))
  ''' % {'tpch_dir': tpch_dir} )