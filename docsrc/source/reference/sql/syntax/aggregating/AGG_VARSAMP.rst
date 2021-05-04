VAR_SAMP
^^^^^^^^

**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`

Calculate the sample variance of a numeric column according to the formula

.. math::
    
    \frac{\sum{(x-\mu)^2}}{N-1}

.. seealso:: :ref:`sql_agg_variance`, :ref:`sql_agg_varpop`

Example
"""""""

.. code-block:: sql

    SELECT <col_1>
        , VAR_SAMP(<col_2>) AS var
    FROM <table_name>
    GROUP BY <col_1>
