.. _sql_conditional:

Conditional operators
---------------------

{% for member in sql.conditional %}
.. _sql_{{ member.lower() }}:
.. include:: syntax/conditional/{{ member }}.rst
{% endfor %} 
 
.. toctree::
    :maxdepth: 2