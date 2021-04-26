CASE expr
^^^^^^^^^

**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`
:ref:`TIMESTAMP<sql_dtypes>`, :ref:`VARCHAR<sql_dtypes>`, :ref:`BOOLEAN<sql_dtypes>`

Compare values in a column to each consecutive :code:`WHEN` clause 
and returns the *first* match. If no match found in any of the 
:code:`WHEN` clauses then, if present, the value from the :code:`ELSE`
clause is returned. If the :code:`ELSE` is not present nor any match
is found, a :code:`NULL` is returned.

Example
"""""""

.. code-block:: sql

    SELECT CASE <col_1> 
            WHEN <literal_1> THEN <result_1>
            WHEN <literal_2> THEN <result_2>
            ELSE <result_3>
    FROM <table_name>


CASE
^^^^

**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`
:ref:`TIMESTAMP<sql_dtypes>`, :ref:`VARCHAR<sql_dtypes>`, :ref:`BOOLEAN<sql_dtypes>`

Compare values in a column to each consecutive :code:`WHEN` clause 
and returns the *first* match. If no match found in any of the 
:code:`WHEN` clauses then, if present, the value from the :code:`ELSE`
clause is returned. If the :code:`ELSE` is not present nor any match
is found, a :code:`NULL` is returned.

The difference between this function and :ref:`sql_func_case` is 
that the conditions can mix more than one column and different :ref:`operators <sql_operators>`.

Example
"""""""

.. code-block:: sql

    SELECT CASE WHEN <col_1> IN (<literal_1>, <literal_2) THEN <result_1>
            WHEN <col_2> > 0 THEN <result_2>
            ELSE <result_3>
    FROM <table_name>
