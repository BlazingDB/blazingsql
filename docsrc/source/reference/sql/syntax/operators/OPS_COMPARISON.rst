Comparison operators
^^^^^^^^^^^^^^^^^^^^

**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`
:ref:`TIMESTAMP<sql_dtypes>`, :ref:`VARCHAR<sql_dtypes>`, :ref:`BOOLEAN<sql_dtypes>`

Comparison operators compare two values and return a truth value: either :code:`TRUE` or :code:`FALSE`. These normally
require columns or literals with comparable data types.

:code:`<`
~~~~~~~~~~

Operator to check if values in one column are less than the values in another, or a literal.

Example
"""""""

.. code-block:: sql

    SELECT <col_1> < <col_2>
    FROM <table_name>

:code:`<=`
~~~~~~~~~~

Operator to check if values in one column are less than or equal to the values in another, or a literal.

Example
"""""""

.. code-block:: sql

    SELECT <col_1> <= <col_2>
    FROM <table_name>

:code:`>`
~~~~~~~~~~

Operator to check if values in one column are more than the values in another, or a literal.

Example
"""""""

.. code-block:: sql

    SELECT <col_1> > <col_2>
    FROM <table_name>

:code:`>=`
~~~~~~~~~~

Operator to check if values in one column are more than or equal to the values in another, or a literal.

Example
"""""""

.. code-block:: sql

    SELECT <col_1> >= <col_2>
    FROM <table_name>

:code:`=`
~~~~~~~~~

Operator to check if values in one column are equal to the values in another, or a literal.

Example
"""""""

.. code-block:: sql

    SELECT <col_1> = <col_2>
    FROM <table_name>

:code:`<>`
~~~~~~~~~~

Operator to check if values in one column are not equal to the values in another, or a literal.

Example
"""""""

.. code-block:: sql

    SELECT <col_1> <> <col_2>
    FROM <table_name>

:code:`BETWEEN`
~~~~~~~~~~~~~~~

Operator to check if values in a column lay in or outside a specified range, specified by either literals or columns.

Examples
""""""""

The range can be specified by literals.

.. code-block:: sql

    SELECT <col_1> BETWEEN <number_1> AND <number_2>
    FROM <table_name>

The range can also be specified by columns.

.. code-block:: sql

    SELECT <col_1> BETWEEN <col_2> AND <col_3>
    FROM <table_name>

To check if values lay outside of the range use :code:`NOT` qualifier.

.. code-block:: sql

    SELECT <col_1> NOT BETWEEN <number_1> AND <number_2>
    FROM <table_name>

:code:`LIKE`
~~~~~~~~~~~~

**Supported datatypes:** :ref:`VARCHAR<sql_dtypes>`

Operator to check if a string matches a pattern specified in the second operand. The :code:`%` sign matches 
any number of characters while the :code:`_` matches only a single character. To match either 
:code:`%`, :code:`_`, or the :code:`\ ` you need to prefix the pattern with another :code:`\ ` thus resulting
in :code:`\%` to match a percent sign, :code:`\_` to match underscore, and :code:`\\` to match a 
backslash.

.. note:: The :code:`LIKE` operator is case-sensitive so to match strings with different casing 
    all strings need to be normalized. See :ref:`LOWER<sql_string_lower>`,
    :ref:`UPPER<sql_string_upper>` or :ref:`INITCAP<sql_string_initcap>`.

Examples
""""""""

Check, if the word finishes with the word `WORK`.

.. code-block:: sql

    SELECT <col_1> LIKE '%WORK'
    FROM <table_name>

Check, if the string matches a phone number pattern.

.. code-block:: sql

    SELECT <col_1> LIKE '(___) ___-____'
    FROM <table_name>

:code:`IN`
~~~~~~~~~~

Operator to check if values in a column are found on a specified list. The list can be literals
or a subquery.
Check the dedicated section on :ref:`sql_ops_in`.