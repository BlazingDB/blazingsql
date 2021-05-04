.. _sql_math:

Mathematical functions
----------------------

Mathematical functions can be applied on values in a numerical column.

{% for member in sql.math %}
.. _sql_{{ member.lower() }}:
.. include:: syntax/math/{{ member }}.rst
{% endfor %}
 
.. toctree::
    :maxdepth: 2