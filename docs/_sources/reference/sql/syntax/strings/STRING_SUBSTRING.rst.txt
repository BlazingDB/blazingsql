SUBSTRING
^^^^^^^^^

**Supported datatypes:** :ref:`VARCHAR<sql_dtypes>`

Obtain a substring of a string with a specified number of characters starting at a specified position.

.. seealso:: :ref:`sql_string_left`, :ref:`sql_string_right`

Example
"""""""

Retrieve 3 characters from a string starting at position 4.

.. code-block:: sql

    SELECT SUBSTRING(<col_1>, 4, 3)
    FROM <table_name>
