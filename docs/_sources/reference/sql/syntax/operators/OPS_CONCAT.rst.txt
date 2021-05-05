:code:`CONCAT` operator
^^^^^^^^^^^^^^^^^^^^^^^

**Supported datatypes:**  :ref:`VARCHAR<sql_dtypes>`

The concatenation operator joins two or more string type values into one. 

.. seealso:: :ref:`sql_string_concat`

:code:`||` operator
~~~~~~~~~~~~~~~~~~~

Combines two or more string type values (columns or literals) into one.

Example
"""""""

Concatenate two text columns.

.. code-block:: sql

    SELECT <col_1> || <col_2>
    FROM <table_name>

Concatenate a column with a string.

.. code-block:: sql

    SELECT <col_1> || <string_literal>
    FROM <table_name>

Add underscore between text values in two columns.

.. code-block:: sql

    SELECT <col_1> || '_' || <col_2>
    FROM <table_name>