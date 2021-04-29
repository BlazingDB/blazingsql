COUNT
^^^^^

**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`
:ref:`TIMESTAMP<sql_dtypes>`, :ref:`VARCHAR<sql_dtypes>`, :ref:`BOOLEAN<sql_dtypes>`

Count the number of rows in each specified group.

.. seealso:: :ref:`sql_agg_avg`, :ref:`sql_agg_sum`

Example
"""""""

The :code:`*` syntax counts all the rows in the table irrespective if :code:`NULL` values.

.. code-block:: sql

    SELECT <col_1>
        , COUNT(*) AS cnt
    FROM <table_name>
    GROUP BY <col_1>

Specifying a column to count over will exclude :code:`NULL` from the final count.

.. code-block:: sql

    SELECT <col_1>
        , COUNT(<col_with_nulls>) AS cnt_no_nulls
    FROM <table_name>
    GROUP BY <col_1>

Adding the :code:`DISTINCT` qualifier will count all the unique values in the 
specified column in each aggregation group.

.. code-block:: sql

    SELECT <col_1>
        , COUNT(DISTINCT <col_2>) AS cnt_unqiue
    FROM <table_name>
    GROUP BY <col_1>