VARIANCE
^^^^^^^^

**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`

Calculate variance of a numeric column according to the formula

.. math::
    
    \frac{\sum{(x-\mu)^2}}{N-1}

.. seealso:: :ref:`sql_agg_varpop`, :ref:`sql_agg_varsamp`

Example
"""""""

.. code-block:: sql

    SELECT <col_1>
        , VARIANCE(<col_2>) AS var
    FROM <table_name>
    GROUP BY <col_1>
