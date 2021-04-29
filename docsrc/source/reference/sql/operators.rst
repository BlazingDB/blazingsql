.. _sql_operators:

Operators
---------

Operators normally use special characters or keywords to manipulate single or multiple operands 
(either scalar, columns, or subqueries). They are a kind of a binary type function that 
requires a minimum of two (or more) operands to return results.

Order of precedence
^^^^^^^^^^^^^^^^^^^

The table below shows the order of precedence for the operators. 

.. list-table:: Order of precedence for operators
    :widths: 10 90
    :header-rows: 1

    * - Order
      - Operators
    * - 1
      - parentheses
    * - 2
      - multiplication, addition, string concatenation
    * - 3
      - addition, subtraction
    * - 4
      - comparison operators, 
    * -
      - :code:`[NOT] LIKE`, :code:`[NOT] BETWEEN`, :code:`[NOT] IN`, 
    * -
      - :code:`IS [NOT] NULL`, :code:`IS [NOT] TRUE`, :code:`IS [NOT] FALSE`
    * - 5
      - :code:`NOT`
    * - 6
      - :code:`AND`
    * - 7
      - :code:`OR`

In case if the operators have the same precedence they are evaluated left-to-right.

{% for member in sql.operators %}
.. _sql_{{ member.lower() }}:
.. include:: syntax/operators/{{ member }}.rst
{% endfor %} 
 
.. toctree::
    :maxdepth: 2