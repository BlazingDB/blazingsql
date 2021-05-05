Arithmetic operators
^^^^^^^^^^^^^^^^^^^^
**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`

Arithmetic operators provide the capability to perform
arithmetic operations on columns or numbers.

Addition
~~~~~~~~

The addition operator performs a sum of two numeric columns or numbers.

Examples
""""""""
Perform addition of elements in two columns.

.. code-block:: sql

    SELECT <col_1> + <col_2>
    FROM <table_name>

It can also add a number to a column

.. code-block:: sql

    SELECT <col_1> + <number>
    FROM <table_name>

or add two or more numbers and columns together.

.. code-block:: sql

    SELECT <col_1> + <col_2> + <number> 
    FROM <table_name>

Subtraction
~~~~~~~~~~~

The subtraction operator subtracts number in one column from the values in another one 
or a number.

Examples
""""""""
Subtract values in one column from another one.

.. code-block:: sql

    SELECT <col_1> - <col_2>
    FROM <table_name>

Subtract a number from a column.

.. code-block:: sql

    SELECT <col_1> - <number>
    FROM <table_name>

Multiplication
~~~~~~~~~~~~~~

The multiplication operator multiplies values between two or more columns or numbers.

Examples
""""""""
Multiply values between two columns.

.. code-block:: sql

    SELECT <col_1> * <col_2>
    FROM <table_name>

Mutliply the values in one column and a number.

.. code-block:: sql

    SELECT <col_1> * <number>
    FROM <table_name>

Division
~~~~~~~~

The division operator takes values from one column and divides them by the values in 
another or a number. 

.. warning:: If the divisor column contains :code:`0` or :code:`NULL` BlazingSQL will return :code:`NULL`. 

Examples
""""""""
Divide values from one column by the values in another column.

.. code-block:: sql

    SELECT <col_1> / <col_2>
    FROM <table_name>

Divide values from one column by the a number.

.. warning:: Dividing explicitly by a literal :code:`0` will throw a :code:`RunExecuteGraphError` exception.

.. code-block:: sql

    SELECT <col_1> * <number>
    FROM <table_name>

Negation
~~~~~~~~

The negation operator returns a negative of a value in a column or of a number.

Examples
""""""""
Negate values in a column.

.. code-block:: sql

    SELECT -<col_1>
    FROM <table_name>