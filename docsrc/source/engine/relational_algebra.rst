Relational algebra
==================


The BlazingSQL engine executes a relational algebra plan. This plan is initially
created by Apache Calcite, which in turn receives a SQL query.
The initial relational algebra is converted into an physical plan,
which is effectively a modified version of the original relational algebra plan,
wherein some of the relational algebra steps is expanded into multiple steps.

SQL
^^^
::

    select o_custkey, sum(o_totalprice) from orders where o_orderkey < 10 group by o_custkey

Relational Algebra
^^^^^^^^^^^^^^^^^^
::

    LogicalProject(o_custkey=[$0], EXPR$1=[CASE(=($2, 0), null:DOUBLE, $1)])
      LogicalAggregate(group=[{0}], EXPR$1=[$SUM0($1)], agg#1=[COUNT($1)])
        LogicalProject(o_custkey=[$1], o_totalprice=[$2])
          BindableTableScan(table=[[main, orders]], filters=[[<($0, 10)]], projects=[[0, 1, 3]], aliases=[[$f0, o_custkey, o_totalprice]])

Physical Plan Single GPU
^^^^^^^^^^^^^^^^^^^^^^^^
::

    LogicalProject(o_custkey=[$0], EXPR$1=[CASE(=($2, 0), null:DOUBLE, $1)])
      MergeAggregate(group=[{0}], EXPR$1=[$SUM0($1)], agg#1=[COUNT($1)])
        ComputeAggregate(group=[{0}], EXPR$1=[$SUM0($1)], agg#1=[COUNT($1)])
          LogicalProject(o_custkey=[$1], o_totalprice=[$2])
            BindableTableScan(table=[[main, orders]], filters=[[<($0, 10)]], projects=[[0, 1, 3]], aliases=[[$f0, o_custkey, o_totalprice]])

Physical Plan Multi GPU
^^^^^^^^^^^^^^^^^^^^^^^
::

    LogicalProject(o_custkey=[$0], EXPR$1=[CASE(=($2, 0), null:DOUBLE, $1)])
      MergeAggregate(group=[{0}], EXPR$1=[$SUM0($1)], agg#1=[COUNT($1)])
        DistributeAggregate(group=[{0}], EXPR$1=[$SUM0($1)], agg#1=[COUNT($1)])
          ComputeAggregate(group=[{0}], EXPR$1=[$SUM0($1)], agg#1=[COUNT($1)])
            LogicalProject(o_custkey=[$1], o_totalprice=[$2])
              BindableTableScan(table=[[main, orders]], filters=[[<($0, 10)]], projects=[[0, 1, 3]], aliases=[[$f0, o_custkey, o_totalprice]])


The conversion of the relational algebra gets done by the function ``transform_json_tree``. 
This function gets called by ``build_batch_graph``.

This new relational algebra plan is converted into a graph and each node in the graph becomes an execution kernel, while each edge becomes a ``CacheMachine``.

The graph is created by ``ral::batch::tree_processor`` that has a function called ``build_batch_graph``. This produces the actual graph object,
which is what contains all the execution kernels and CacheMachines. The graph has a function called ``execute()`` which is what actually starts the ``run()`` function of every execution kernel, each on its own thread.