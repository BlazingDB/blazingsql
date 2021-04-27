REPLACE
^^^^^^^

**Supported datatypes:** :ref:`VARCHAR<sql_dtypes>`

Obtain a string with all instances of a match substring are replaced with a specified
replacement.

.. seealso:: :ref:`sql_string_regexpreplace`

Example
"""""""

.. code-block:: sql

    SELECT REPLACE(<col_1>, <substring>, <replacement>)
    FROM <table_name>
