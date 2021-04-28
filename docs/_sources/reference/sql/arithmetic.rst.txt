.. _sql_arithmetic:

Arithmetic functions
====================

{% for member in sql.arithmetic %}
.. _sql_{{ member.lower() }}:
.. include:: syntax/arithmetic/{{ member }}.rst
{% endfor %}
 
.. toctree::
    :maxdepth: 2