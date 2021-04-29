.. _sql_query:

Querying data
=============

Query statements like :ref:`SELECT <sql_agg_avg>` scan one or more data tables registered with 
:code:`BlazingContext` and return results of the engine run. In this section we describe
the most fundamental SQL functionality: selecting rows. 

{% for member in sql.query %}
.. _sql_{{ member.lower() }}:
.. include:: syntax/query/{{ member }}.rst
{% endfor %} 

.. toctree::
    :maxdepth: 2