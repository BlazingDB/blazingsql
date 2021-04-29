RIGHT
^^^^^

**Supported datatypes:** :ref:`VARCHAR<sql_dtypes>`

Obtain a substring of a string with a specified number of rightmost characters.

.. seealso:: :ref:`sql_string_left`, :ref:`sql_string_substring`

Example
"""""""

Retrieve last 3 characters from a string.

.. code-block:: sql

    SELECT RIGHT(<col_1>, 3)
    FROM <table_name>
