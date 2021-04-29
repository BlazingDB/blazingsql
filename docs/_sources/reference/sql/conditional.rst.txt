.. _sql_conditional:

Conditional functions
---------------------

Conditional functions allow encoding conditional expressions
and make decisions based on the values found in a column.

{% for member in sql.conditional %}
.. _sql_{{ member.lower() }}:
.. include:: syntax/conditional/{{ member }}.rst
{% endfor %} 
 
.. toctree::
    :maxdepth: 2