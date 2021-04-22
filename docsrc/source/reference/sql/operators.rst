.. _sql_operators:

Operators
---------

{% for member in sql.operators %}
.. _sql_{{ member.lower() }}:
.. include:: syntax/operators/{{ member }}.rst
{% endfor %} 
 
.. toctree::
    :maxdepth: 2