LEFT
^^^^

**Supported datatypes:** :ref:`VARCHAR<sql_dtypes>`

Obtain a substring of a string with a specified number of leftmost characters.

.. seealso:: :ref:`sql_string_right`, :ref:`sql_string_substring`

Example
"""""""

Retrieve first 3 characters from a string.

.. code-block:: sql

    SELECT LEFT(<col_1>, 3)
    FROM <table_name>
