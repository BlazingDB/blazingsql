.. _sql_aggregating:

Aggregating data
================

Aggregating data using SQL is done using :code:`GROUP BY` function.

GROUP BY function
-----------------

The :code:`GROUP BY` function allows specifying single column or multiple
columns as level of aggregation.

.. code-block:: sql

    SELECT <col_1>
        , <col_2>
        , COUNT(*) AS cnt
    FROM <table_name>
    GROUP BY <col_1>
        , <col_2>

Additional predicates and clauses can be added, too

.. code-block:: sql

    SELECT <col_1>
        , <col_2>
        , COUNT(*) AS cnt
    FROM <table_name>
    WHERE <col_2> <= 0
    GROUP BY <col_1>
        , <col_2>
    ORDER BY <col_1> ASC
        , <col_2> DESC

Since :code:`GROUP BY` by definition returns a list of unique values found in the columns used to 
specify the aggregation level there is no need to add the :ref:`DISTINCT<sql_select_distinct>` qualifier.
However, the following syntax is also supported.

.. code-block:: sql

    SELECT DISTINCT <col_1>
        , <col_2>
        , COUNT(*) AS cnt
    FROM <table_name>
    GROUP BY <col_1>
        , <col_2>

HAVING qualifier
-----------------

The difference between the :ref:`WHERE<sql_where>` statement and the :code:`HAVING` clause 
is the order of operations: 

1. with the :code:`WHERE` predicate, the table rows get filtered first and then the aggregating function is applied.
2. the :code:`HAVING` clause applies the filter to already aggregated results that is any of the :ref:`aggregating functions<sql_agg_funcs>`

.. code-block:: sql

    SELECT <col_1>
        , <col_2>
        , COUNT(*) AS cnt
    FROM <table_name>
    GROUP BY <col_1>
        , <col_2>
    HAVING COUNT(*) > 100

Both, the :code:`WHERE` and the :code:`HAVING` statements can appear in the same query.

.. code-block:: sql

    SELECT <col_1>
        , <col_2>
        , COUNT(*) AS cnt
    FROM <table_name>
    WHERE <col_2> <= 0
    GROUP BY <col_1>
        , <col_2>
    HAVING COUNT(*) > 100
    

.. _sql_agg_funcs:

Aggregating functions
---------------------

Below is a full list of currently supported aggregation functions by BlazingSQL.

{% for member in sql.aggregating %}
.. _sql_{{ member.lower() }}:
.. include:: syntax/aggregating/{{ member }}.rst
{% endfor %} 


.. toctree::
    :maxdepth: 2