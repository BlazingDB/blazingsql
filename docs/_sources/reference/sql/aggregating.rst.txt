.. _sql_aggregating:

Aggregating data
================

{% for member in sql.aggregating %}
.. _sql_{{ member.lower() }}:
.. include:: syntax/aggregating/{{ member }}.rst
{% endfor %} 
 
.. toctree::
    :maxdepth: 2