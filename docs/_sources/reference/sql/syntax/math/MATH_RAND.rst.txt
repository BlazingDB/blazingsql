RAND
^^^^

**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`

Obtain a random number in the rage :math:`[0,1)` (includes :code:`0` but excludes :code:`1`).
This function takes no parameters.

Example
"""""""

.. code-block:: sql

    SELECT RAND()
    FROM <table_name>