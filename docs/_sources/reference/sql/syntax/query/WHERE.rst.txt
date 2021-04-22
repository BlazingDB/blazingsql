WHERE function
^^^^^^^^^^^^^^

**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`
:ref:`TIMESTAMP<sql_dtypes>`, :ref:`VARCHAR<sql_dtypes>`, :ref:`BOOLEAN<sql_dtypes>`

Filter returned rows from a table. The following operators are supported in :code:`WHERE` statements: :ref:`comparison<sql_ops_comparison>`
, :ref:`logical<sql_ops_logical>`, and :ref:`IN operators<sql_ops_in>` and :ref:`IS operators<sql_ops_comparison>`.

Examples
""""""""

Comparison operators
~~~~~~~~~~~~~~~~~~~~

These include operators like `less than` or `equal to`. Refer to :ref:`comparison operators<sql_ops_comparison>` for a full list of supported
syntax.

.. code-block:: sql

    SELECT *
    FROM <table_name>
    WHERE <col_1> = <number>

.. code-block:: sql

    SELECT *
    FROM <table_name>
    WHERE <col_1> <= <number>

Selecting rows can also be done by comparing values in columns with comparable data types: numeric with numeric, timestamp with timestamp 
, string with string (only :code:`=` is supported).

.. code-block:: sql

    SELECT *
    FROM <table_name>
    WHERE <col_1> >= <col_2>

Logical operators
~~~~~~~~~~~~~~~~~

When using the :code:`WHERE` statement rows are selected where the predicate for the row 
evaluates to :code:`TRUE`: in first the example above only rows where :code:`col_1`
are equal to some :code:`number` will be selected. 

These statements can be further *chained* to create more complex logical statements.

.. code-block:: sql

    SELECT *
    FROM <table_name>
    WHERE <col_1> => <number_1>
        AND <col_2> <= <number_2>

.. code-block:: sql

    SELECT *
    FROM <table_name>
    WHERE <col_1> BETWEEN <number_1> AND <number_2>
        OR <col_2> <= <number_3>

Refer to :ref:`logical operators<sql_ops_logical>` for a full list of supported
syntax.

:code:`IN` operators
~~~~~~~~~~~~~~~~~~~~

Filtering data to some subset of allowed values can be achieved with 
:code:`IN` operators.

.. code-block:: sql

    SELECT *
    FROM <table_name>
    WHERE <col_1_string> IN (<string_1>, <string_2>)

The above example is an equivalent to:

.. code-block:: sql

    SELECT *
    FROM <table_name>
    WHERE <col_1_string> = <string_1>
        OR <col_1_string> = <string_2>

To select all the rows but those that are equal to :code:`string_1` or
:code:`string_2`, the :code:`IN` statement can be negated:

.. code-block:: sql

    SELECT *
    FROM <table_name>
    WHERE <col_1_string> NOT IN (<string_1>, <string_2>)

If the list of strings is long a subquery can be included within the parentheses:

.. code-block:: sql

    SELECT *
    FROM <table_1>
    WHERE <col_1_string> IN (
        SELECT <col_2_string>
        FROM <table_2>
        WHERE <col_1> >= <number>
    )

Refer to :ref:`IN operators<sql_ops_in>` for a full list of supported
syntax.

:code:`IS` operators 
~~~~~~~~~~~~~~~~~~~~~

Filtering data using :code:`BOOLEAN` columns can be achieved by simply passing the column itself
(or its negation) as a predicate:

.. code-block:: sql

    SELECT *
    FROM <table_name>
    WHERE <col_1>

.. code-block:: sql

    SELECT *
    FROM <table_name>
    WHERE NOT <col_1>

The first example above code is equivalent to 

.. code-block:: sql

    SELECT *
    FROM <table_name>
    WHERE <col_1> IS TRUE 

or 

.. code-block:: sql

    SELECT *
    FROM <table_name>
    WHERE <col_1> IS NOT FALSE

Refer to :ref:`IS operators<sql_ops_is>` for a full list of supported
syntax.