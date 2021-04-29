ORDER BY function 
^^^^^^^^^^^^^^^^^

**Supported datatypes:** :ref:`TINYINT<sql_dtypes>`, :ref:`SMALLINT<sql_dtypes>`, :ref:`INT<sql_dtypes>`, :ref:`BIGINT<sql_dtypes>`, :ref:`DECIMAL<sql_dtypes>`, :ref:`FLOAT<sql_dtypes>`, :ref:`DOUBLE<sql_dtypes>`
:ref:`TIMESTAMP<sql_dtypes>`, :ref:`VARCHAR<sql_dtypes>`, :ref:`BOOLEAN<sql_dtypes>`

Sort the returned rows in a specified order.

Examples
""""""""

:code:`ORDER BY`
~~~~~~~~~~~~~~~~~~~~~~

Ordering by columns using :code:`ORDER BY <col>`

.. code-block:: sql

    SELECT *
    FROM <foo>
    ORDER BY <col_1>

.. code-block:: sql

    SELECT *
    FROM <foo>
    ORDER BY <col_1>, <col_2>

Monotonicity qualifiers
~~~~~~~~~~~~~~~~~~~~~~~

Ordering ascending or descending using :code:`ASC` and :code:`DESC` qualifiers.

.. code-block:: sql

    SELECT *
    FROM <foo>
    ORDER BY <col_1> ASC

.. code-block:: sql

    SELECT *
    FROM <foo>
    ORDER BY <col_1> DESC

These qualifiers can also be mixed

.. code-block:: sql

    SELECT *
    FROM <foo>
    ORDER BY <col_1> ASC, <col_2> DESC

:code:`NULL` values sorting
~~~~~~~~~~~~~~~~~~~~~~~~~~~

If the columns contain :code:`NULL` values, by default, the rows with :code:`NULL` values will be placed last.
This behavior can be changed explicitly using :code:`NULLS FIRST` or :code:`NULLS LAST` qualifiers:

.. code-block:: sql

    SELECT *
    FROM <foo>
    ORDER BY <col_1> NULLS LAST -- default

.. code-block:: sql

    SELECT *
    FROM <foo>
    ORDER BY <col_1> NULLS FIRST

These qualifiers are also supported for multi-column sorts with monotonicity qualifiers:

.. code-block:: sql

    SELECT *
    FROM <foo>
    ORDER BY <col_1> ASC NULLS FIRST
        , <col_2> DESC NULLS LAST

