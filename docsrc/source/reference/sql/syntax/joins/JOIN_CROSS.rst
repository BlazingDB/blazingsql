CROSS
^^^^^

**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`
:ref:`TIMESTAMP<sql_dtypes>`, :ref:`VARCHAR<sql_dtypes>`, :ref:`BOOLEAN<sql_dtypes>`

The :code:`CROSS JOIN` returns a cartesian product of the selected columns thus it should be 
used with caution as the number of results is a product of the unique numbers in each of the columns.

Example
"""""""

.. code-block:: sql

    SELECT A.<col_1>
        , B.<col_2>
    FROM <table_1> AS A
    CROSS JOIN <table_2> AS B

Using the tables defined :ref:`above<sql_joins_tables>`, The following code 

.. code-block:: sql

    SELECT A.A AS col_1
        , B.A AS col_2
    FROM table_1 AS A
    CROSS JOIN table_2 AS B

will return 

.. list-table:: Table 2
    :widths: 33 33
    :header-rows: 1

    * - col_1
      - col_2
    * - 1
      - 1 
    * - 1
      - 2
    * - 1
      - 3
    * - 2
      - 1 
    * - 2
      - 2
    * - 2
      - 3
    * - 3
      - 1 
    * - 3
      - 2
    * - 3
      - 3
