FIRST_VALUE
~~~~~~~~~~~

**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`
:ref:`TIMESTAMP<sql_dtypes>`, :ref:`VARCHAR<sql_dtypes>`, :ref:`BOOLEAN<sql_dtypes>`

Return the first value from the specified window.

.. warning:: This function includes :code:`NULL` values and if the :code:`NULL` values is  
    first, :code:`NULL` will be returned.

.. seealso:: :ref:`sql_window_lastvalue`

Example
"""""""

.. code-block:: sql

    SELECT <col_1>
        , FIRST_VALUE(<col_2>) OVER (
            PARTITION BY <col_2> 
            ORDER BY <col_3>
        )
    FROM <table_name>