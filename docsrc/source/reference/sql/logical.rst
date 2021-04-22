.. _sql_functions:

Conditional functions
=====================

{% for member in sql.functions %}
.. _sql_{{ member.lower() }}:
.. include:: syntax/conditional/{{ member }}.rst
{% endfor %} 
 
.. toctree::
    :maxdepth: 2