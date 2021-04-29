FULL OUTER
^^^^^^^^^^

**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`
:ref:`TIMESTAMP<sql_dtypes>`, :ref:`VARCHAR<sql_dtypes>`, :ref:`BOOLEAN<sql_dtypes>`

The :code:`FULL OUTER JOIN` returns rows from both tables for all distinct values 
found in the matching column or columns. :code:`NULL` values are returned for not matched 
rows from either of the tables.

.. seealso:: :ref:`sql_join_leftouter`

Example
"""""""

.. code-block:: sql

    SELECT A.<col_1>
        , B.<col_2>
    FROM <table_1> AS A
    FULL OUTER JOIN <table_2> AS B
        ON A.<col_3> = B.<col_3>

Using the tables defined :ref:`above<sql_joins_tables>`, The following code 

.. code-block:: sql

    SELECT A.A AS col_1
        , B.A AS col_2
        , B.B AS col_3
    FROM table_1 AS A
    FULL OUTER JOIN table_2 AS B
        ON A.A = B.B

will return 

.. list-table:: Table 2
    :widths: 33 33 33
    :header-rows: 1

    * - col_1
      - col_2
      - col_3
    * - 1
      - :code:`NULL`
      - :code:`NULL`
    * - 2
      - 1
      - 2
    * - 3
      - 2
      - 3
    * - :code:`NULL`
      - 3
      - 4
