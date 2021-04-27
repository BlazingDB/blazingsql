DAYOFWEEK
^^^^^^^^^

**Supported datatypes:** :ref:`TIMESTAMP<sql_dtypes>`

Extract the day of the week from a :code:`TIMESTAMP` column.

.. note:: Returned values range :math:`[1,7]`

.. seealso:: :ref:`sql_date_extract`

Example
"""""""

.. code-block:: sql

    SELECT DAYOFMONTH(<col_1>)
    FROM <table_name>
