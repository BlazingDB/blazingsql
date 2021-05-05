Logical operators
^^^^^^^^^^^^^^^^^

**Supported datatypes:** :ref:`BOOLEAN<sql_dtypes>`

Logical operators allow to chain multiple boolean columns or expressions that return boolean
value to obtain more complex logical expressions.

.. _sql_logical_and:

:code:`AND`
~~~~~~~~~~~

The logical conjunction operator returns :code:`TRUE` value if and only if both 
inputs are :code:`TRUE`.

.. list-table:: Truth table for :code:`AND`
    :widths: auto
    :header-rows: 1

    * - Input 1
      - Input 2
      - Result
    * - :code:`FALSE`
      - :code:`FALSE`
      - :code:`FALSE`
    * - :code:`FALSE`
      - :code:`TRUE`
      - :code:`FALSE`
    * - :code:`TRUE`
      - :code:`FALSE`
      - :code:`FALSE`
    * - :code:`TRUE`
      - :code:`TRUE`
      - :code:`TRUE`
    * - :code:`FALSE`
      - :code:`NULL`
      - :code:`FALSE`
    * - :code:`TRUE`
      - :code:`NULL`
      - :code:`NULL`
    * - :code:`NULL`
      - :code:`TRUE`
      - :code:`NULL`
    * - :code:`NULL`
      - :code:`FALSE`
      - :code:`FALSE`
    * - :code:`NULL`
      - :code:`NULL`
      - :code:`NULL`

.. seealso:: :ref:`sql_logical_or`, :ref:`sql_logical_not`

Examples
""""""""

Simple conjunction of two boolean columns.

.. code-block:: sql

    SELECT <col_1_bool> AND <col_2_bool>
    FROM <table_name>

Negated column syntax.

.. code-block:: sql

    SELECT <col_1_bool> AND NOT <col_2_bool>
    FROM <table_name>

.. _sql_logical_or:

:code:`OR`
~~~~~~~~~~

The logical disjunction (or alternative) operator returns :code:`TRUE` value if one 
of the inputs is :code:`TRUE`.

.. list-table:: Truth table for :code:`OR`
    :widths: auto
    :header-rows: 1

    * - Input 1
      - Input 2
      - Result
    * - :code:`FALSE`
      - :code:`FALSE`
      - :code:`FALSE`
    * - :code:`FALSE`
      - :code:`TRUE`
      - :code:`TRUE`
    * - :code:`TRUE`
      - :code:`FALSE`
      - :code:`TRUE`
    * - :code:`TRUE`
      - :code:`TRUE`
      - :code:`TRUE`
    * - :code:`FALSE`
      - :code:`NULL`
      - :code:`NULL`
    * - :code:`TRUE`
      - :code:`NULL`
      - :code:`TRUE`
    * - :code:`NULL`
      - :code:`TRUE`
      - :code:`TRUE`
    * - :code:`NULL`
      - :code:`FALSE`
      - :code:`NULL`
    * - :code:`NULL`
      - :code:`NULL`
      - :code:`NULL`

.. seealso:: :ref:`sql_logical_and`, :ref:`sql_logical_not`

Examples
""""""""

Simple disjunction of two boolean columns.

.. code-block:: sql

    SELECT <col_1_bool> OR <col_2_bool>
    FROM <table_name>

Negated column syntax.

.. code-block:: sql

    SELECT <col_1_bool> OR NOT <col_2_bool>
    FROM <table_name>

.. _sql_logical_not:

:code:`NOT`
~~~~~~~~~~~

The logical negation operator returns the reverse truth value.

.. list-table:: Truth table for :code:`NOT`
    :widths: auto
    :header-rows: 1

    * - Input
      - Result
    * - :code:`FALSE`
      - :code:`TRUE`
    * - :code:`TRUE`
      - :code:`FALSE`
    * - :code:`NULL`
      - :code:`NULL`

.. seealso:: :ref:`sql_logical_and`, :ref:`sql_logical_or`

Example
"""""""

Simple negation of a boolean column.

.. code-block:: sql

    SELECT NOT <col_1_bool>
    FROM <table_name>