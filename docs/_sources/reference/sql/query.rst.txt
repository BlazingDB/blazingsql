.. _sql_query:

Querying data
=============

Query statements like :ref:`SELECT <sql_select>` scan one or more data tables registered with 
:code:`BlazingContext` and return results of the engine run. In this section we describe
the most fundamental SQL functionality: selecting rows. 

Selecting data
--------------

{% for member in sql.query %}
.. _sql_{{ member.lower() }}:
.. include:: syntax/query/{{ member }}.rst
{% endfor %} 

Nested queries
--------------

BlazingSQL supports nested queries that might first apply predicates in the subquery 
and and later join it with another table or subquery.

.. code-block:: sql

    SELECT A.<col_1>
        , B.<col_3>
    FROM (
        SELECT <col_1>
            , <col_2>
        FROM <table_1>
        WHERE <col_2> < 0
    ) AS A
    INNER JOIN <table_2> AS B
        ON A.<col_2> = B.<col_2>

:code:`WITH` function
---------------------

Complex queries with multiple nested subqueries can quickly become
unreadable. BlazingSQL supports organizing complex queries with the
:code:`WITH` qualifier.

.. code-block:: sql

    WITH <subquery_1> AS (
        SELECT <col_1>
            , <col_2>
        FROM <table_1>
        WHERE <col_2> < 0
    ), <subquery_2> AS (
        SELECT B.<col_1>
            , A.<col_3>
        FROM <table_2> AS A
        INNER JOIN <subquery_1> AS B
            ON A.<col_2> = B.<col_2>
    )
    SELECT *
    FROM <subquery_2>

.. toctree::
    :maxdepth: 2