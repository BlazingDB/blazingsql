.. _sql_windowing:

Analytic functions
==================

Analytic functions compute values over a list (or a *sliding window*) of rows and return 
a computed value for each row. 

The :code:`OVER` clause defines a window of rows that are being using to calculate the result:
for each row the window is different and the aggregating function returns the result 
calculated over that set of rows.

Analytic functions allow computing moving averages, cumulative sums, or extract first or last values
from a defined window.

.. seealso:: Contrast with :ref:`aggregating functions <sql_aggregating>` that return
    a result for a whole group of rows.

Defining window
^^^^^^^^^^^^^^^

In order to use analytic functions a window needs to be defined.

.. warning:: While ANSI SQL supports windows without specifying partitions 
    currently BlazingSQL requires specifying the :code:`PARTITION BY` clause.

Syntax
~~~~~~

The syntax to use analytic functions in BlazingSQL follows the ANSI standard:

.. code-block::

    SELECT <col_1>
        , <col_2>
        , <analytic_function> OVER (
            PARTITION BY <col_1> 
            [ ORDER BY <col_3> { ASC | DESC } { NULLS FIRST | NULLS LAST } }] 
            [ ROWS | RANGE ] 
        )
    FROM <table_name>

:code:`PARTITION BY`
~~~~~~~~~~~~~~~~~~~~

The :code:`PARTITION BY` clause specifies the column (or columns) that 
the window function will further delineate the boundaries of rows; these boundaries
can further be refined by using :ref:`sql_window_rowsrange` qualifiers.

:code:`ORDER BY`
~~~~~~~~~~~~~~~~

The rows within the specified can be ordered (some of the aggregating functions 
require the rows to be sorted). Just like with the reqular :code:`SELECT` statement, 
the :code:`ORDER BY` clause can be used within the specified window. 

:code:`ASC` and :code:`DESC` qualifiers
"""""""""""""""""""""""""""""""""""""""

The rows within window can be sorted by one or more column, and can be mixed and 
matched in terms of the monotonicity.

.. note:: The :code:`ASC` qualifier is explicit and informs the engine to sort
    the rows in an ascending order. However, if omitted, the engine implicitly
    orders the rows in and ascending order. To sort in a descending order the 
    :code:`DESC` is required.

As an example, the code below sorts the results by :code:`col_3` descending and
then by :code:`col_4` ascending.

.. code-block:: sql 

    SELECT col_1
        , col_2
        , SUM(col_5) OVER (
            PARTITION BY col_1
            ORDER BY col_3 DESC 
                , col_4 ASC 
        )

Handling of :code:`NULL` values
"""""""""""""""""""""""""""""""

If the columns used to sort the rows contain :code:`NULL` values certain 
aggregating functions will return different result depending on where the 
:code:`NULL` values appear: either on start or end of the defined window.
BlazingSQL supports specifying this explicitly using :code:`NULLS FIRST`
or :code:`NULLS LAST` clauses.

As an example, the code below sorts the results by :code:`col_3` descending and 
*pushes* the :code:`NULL` values to be top so they appear first in the order.

.. code-block:: sql 

    SELECT col_1
        , col_2
        , SUM(col_5) OVER (
            PARTITION BY col_1
            ORDER BY col_3 DESC NULLS FIRST
        )

.. _sql_window_rowsrange:

:code:`ROWS` or :code:`RANGE`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The partitioning window can further be refined to delineate the range 
that the window covers using the :code:`ROWS` or :code:`RANGE`. 

The :code:`ROWS` clause uses the exact specified number of rows to determine the 
window boundries. As an example, it can include 1 row prior and 
2 rows after the current row. 
The :code:`RANGE` clause, on the other hand, determines the window based 
on a logical range of rows around the current one; the logic of which 
rows to include is derived from the :code:`ORDER BY` column.

.. warning:: The :code:`RANGE` is not currently supported.

:code:`PRECEDING` and :code:`FOLLOWING` qualifiers
""""""""""""""""""""""""""""""""""""""""""""""""""

The :code:`ROWS` or :code:`RANGE` qualifiers are normally followed
by offset qualifiers. These can specify either backward inclusion using
the :code:`PRECEDING` clause, for example 

.. code-block:: sql 

    SELECT col_1
        , col_2
        , SUM(col_5) OVER (
            PARTITION BY col_1
            ORDER BY col_3 DESC NULLS FIRST
            ROWS 1 PRECEDING
        )

or 

.. code-block:: sql 

    SELECT col_1
        , col_2
        , SUM(col_5) OVER (
            PARTITION BY col_1
            ORDER BY col_3 DESC NULLS FIRST
            ROWS UNBOUNDED PRECEDING
        )

The latter example includes all the rows for the partition 
up to the current row while the former includes only one row prior.

Similarly, a forward looking behavior can be achieved.

.. code-block:: sql 

    SELECT col_1
        , col_2
        , SUM(col_5) OVER (
            PARTITION BY col_1
            ORDER BY col_3 DESC NULLS FIRST
            ROWS 1 FOLLOWING
        )

but this functionality requires specifying an exact number of rows.

:code:`BETWEEN`
"""""""""""""""

It is also possible to specify a symmetrical or asymmetrical window around
the current row.

.. code-block:: sql 

    SELECT col_1
        , col_2
        , SUM(col_5) OVER (
            PARTITION BY col_1
            ORDER BY col_3 DESC NULLS FIRST
            ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
        )

A special case where :code:`CURRENT ROW` is specified explicitly can be used 
to create a backward

.. code-block:: sql 

    SELECT col_1
        , col_2
        , SUM(col_5) OVER (
            PARTITION BY col_1
            ORDER BY col_3 DESC NULLS FIRST
            ROWS BETWEEN 1 PRECEDING AND CURERENT ROW

of forward looking windows.

.. code-block:: sql 

    SELECT col_1
        , col_2
        , SUM(col_5) OVER (
            PARTITION BY col_1
            ORDER BY col_3 DESC NULLS FIRST
            ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING

Supported aggregating functions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

{% for member in sql.windowing %}
.. _sql_{{ member.lower() }}:
.. include:: syntax/windowing/{{ member }}.rst
{% endfor %} 
 
.. toctree::
    :maxdepth: 2