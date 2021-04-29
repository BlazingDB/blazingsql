LEFT OUTER
^^^^^^^^^^

**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`
:ref:`TIMESTAMP<sql_dtypes>`, :ref:`VARCHAR<sql_dtypes>`, :ref:`BOOLEAN<sql_dtypes>`

The :code:`LEFT OUTER JOIN` returns rows from both tables for values
from the *left* table and all matching values in the *right* table
that are matching based on the matching criteria. :code:`NULL` values are returned for not matched 
rows from the *right* table.

.. seealso:: :ref:`sql_join_fullouter`

Example
"""""""

.. code-block:: sql

    SELECT A.<col_1>
        , B.<col_2>
    FROM <table_1> AS A
    LEFT OUTER JOIN <table_2> AS B
        ON A.<col_3> = B.<col_3>

Using the tables defined :ref:`above<sql_joins_tables>`, The following code 

.. code-block:: sql

    SELECT A.A AS col_1
        , B.A AS col_2
        , B.B AS col_3
    FROM table_1 AS A
    LEFT OUTER JOIN table_2 AS B
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