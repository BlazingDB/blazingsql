.. _sql_query:

Querying data
=============

Query statements like :ref:`sql_select` scan one or more data tables registered with 
`BlazingContext` and return results of the engine run. In this section we describe
the most fundamental SQL functionality: selecting rows.

{% for member in sql.query %}
.. include:: syntax/query/{{ member }}.rst
{% endfor %} 

.. toctree::
    :maxdepth: 2