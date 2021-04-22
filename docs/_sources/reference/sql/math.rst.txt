.. _sql_math:

Mathematical functions
----------------------

{% for member in sql.math %}
.. _sql_{{ member.lower() }}:
.. include:: syntax/math/{{ member }}.rst
{% endfor %}
 
.. toctree::
    :maxdepth: 2