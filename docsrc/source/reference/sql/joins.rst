.. _sql_joins:

Joining data
============

For performance and size reasons frequently data is *split* into multiple tables.
:code:`JOIN` operations merge two tables based on a matching column or columns 
and allow returning columns from both tables.

.. _sql_joins_tables:

In the examples below we will use the following tables:

:code:`table_1` is 

.. list-table:: Table 1
    :widths: 50 50
    :header-rows: 1

    * - A
      - B
    * - 1
      - 2 
    * - 2
      - 3
    * - 3
      - 4

and :code:`table_2` is 

.. list-table:: Table 2
    :widths: 33 33 34
    :header-rows: 1

    * - A
      - B
      - C
    * - 1
      - 2 
      - 3
    * - 2
      - 3
      - 4
    * - 3
      - 4
      - 5

{% for member in sql.joins %}
.. _sql_{{ member.lower() }}:
.. include:: syntax/joins/{{ member }}.rst
{% endfor %} 
 
.. toctree::
    :maxdepth: 2