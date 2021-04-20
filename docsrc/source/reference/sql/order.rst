.. _sql_order:

Ordering data
=============

{% for member in sql.order %}
.. _sql_{{ member.lower() }}:
.. include:: syntax/order/{{ member }}.rst
{% endfor %} 
 
.. toctree::
    :maxdepth: 2