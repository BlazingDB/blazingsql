STDDEV_SAMP
^^^^^^^^^^^

**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`

Calculate the sample standard deviation of a numeric column according to the formula

.. math::
    
    \sqrt{\frac{\sum{(x-\mu)^2}}{N-1}}

.. seealso:: :ref:`sql_agg_stddev`, :ref:`sql_agg_stddevpop`

Example
"""""""

.. code-block:: sql

    SELECT <col_1>
        , STDDEV_SAMP(<col_2>) AS standard_deviation
    FROM <table_name>
    GROUP BY <col_1>
