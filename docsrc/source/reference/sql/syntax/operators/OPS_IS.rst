:code:`IS` operators
^^^^^^^^^^^^^^^^^^^^

The :code:`IS` operators test if something is or is not. These operators never return :code:`NULL`.

:code:`IS [NOT] NULL`
~~~~~~~~~~~~~~~~~~~~~

**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`
:ref:`TIMESTAMP<sql_dtypes>`, :ref:`VARCHAR<sql_dtypes>`, :ref:`BOOLEAN<sql_dtypes>`

Tests if values in the column are :code:`NULL`.

Example
"""""""

Check if values are :code:`NULL`.

.. code-block:: sql

    SELECT <col_1> IS NULL
    FROM <table_name>

Check if values are not :code:`NULL`.


.. code-block:: sql

    SELECT <col_1> IS NULL
    FROM <table_name>

:code:`IS [NOT] TRUE` and :code:`IS [NOT] FALSE`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Supported datatypes:** :ref:`BOOLEAN<sql_dtypes>`

Tests if values in the column are :code:`TRUE` or :code:`FALSE`.

Example
"""""""

Check if values are :code:`TRUE`.

.. code-block:: sql

    SELECT <col_1> IS TRUE
    FROM <table_name>

An equivalent would be to test if the values are not :code:`FALSE`

.. code-block:: sql

    SELECT <col_1> IS NOT FALSE
    FROM <table_name>

Check if values are :code:`FALSE`.

.. code-block:: sql

    SELECT <col_1> IS FALSE
    FROM <table_name>

Alternatively:

.. code-block:: sql

    SELECT <col_1> IS NOT TRUE
    FROM <table_name>