#include "tests/utilities/BlazingUnitTest.h"
#include "execution_graph/logic_controllers/PhysicalPlanGenerator.h"
#include <transport/Node.h>
#include "execution_graph/Context.h"

using blazingdb::transport::Node;
using Context = blazingdb::manager::Context;

struct PhysicalPlanGeneratorTest : public BlazingUnitTest {};

TEST_F(PhysicalPlanGeneratorTest, transform_json_tree_one_join)
{
	//	Query
	// 	select * from product left join client on client.id_client = product.id_client
	//
	//  Optimized Plan
	//	LogicalJoin(condition=[=($3, $1)], joinType=[left])
	//		LogicalTableScan(table=[[main, product]])
	//		LogicalTableScan(table=[[main, client]])

	std::string logicalPlan =
	R"raw(
	{
		"expr": "LogicalJoin(condition=[=($3, $1)], joinType=[left])",
		"children": [
			{
				"expr": "LogicalTableScan(table=[[main, product]])",
				"children": []
			},
			{
				"expr": "LogicalTableScan(table=[[main, client]])",
				"children": []
			}
		]
	}
	)raw";

	Context context(0, {}, {}, logicalPlan, {});
	ral::batch::tree_processor tree{{}, context.clone(), {}, {}, {}, {}, true};

	std::istringstream input(logicalPlan);
	boost::property_tree::ptree p_tree;
	boost::property_tree::read_json(input, p_tree);
	tree.transform_json_tree(p_tree);

	std::string jsonCompare =
	R"raw(
	{
		"expr":"PartwiseJoin(condition=[=($3, $1)], joinType=[left])",
		"children": [
			{
				"expr":"JoinPartition(condition=[=($3, $1)], joinType=[left])",
				"children": [
					{
						"expr":"LogicalTableScan(table=[[main, product]])",
						"children":""
					},
					{
						"expr":"LogicalTableScan(table=[[main, client]])",
						"children":""
					}
				]
			}
		]
	}
	)raw";

	std::istringstream inputcmp(jsonCompare);
	boost::property_tree::ptree p_tree_cmp;
	boost::property_tree::read_json(inputcmp, p_tree_cmp);

	ASSERT_EQ(p_tree, p_tree_cmp);
}

TEST_F(PhysicalPlanGeneratorTest, transform_json_tree_two_join)
{
	//	Query
	// 	select * from product left join client on client.id_client = product.id_client left join preference on preference.id_product = product.id_product
	//
	//  Optimized Plan
	//	LogicalJoin(condition=[=($6, $0)], joinType=[left])
	//		LogicalJoin(condition=[=($3, $1)], joinType=[left])
	//			LogicalTableScan(table=[[main, product]])
	//			LogicalTableScan(table=[[main, client]])
	//		LogicalTableScan(table=[[main, preference]])

	std::string logicalPlan =
	R"raw(
	{
		"expr": "LogicalJoin(condition=[=($6, $0)], joinType=[left])",
		"children": [
			{
				"expr": "LogicalJoin(condition=[=($3, $1)], joinType=[left])",
				"children": [
					{
						"expr": "LogicalTableScan(table=[[main, product]])",
						"children": []
					},
					{
						"expr": "LogicalTableScan(table=[[main, client]])",
						"children": []
					}
				]
			},
			{
				"expr": "LogicalTableScan(table=[[main, preference]])",
				"children": []
			}
		]
	}
	)raw";

	Context context(0, {}, {}, logicalPlan, {});
	ral::batch::tree_processor tree{{}, context.clone(), {}, {}, {}, {}, true};

	std::istringstream input(logicalPlan);
	boost::property_tree::ptree p_tree;
	boost::property_tree::read_json(input, p_tree);
	tree.transform_json_tree(p_tree);

	std::string jsonCompare =
	R"raw(
	{
		"expr": "PartwiseJoin(condition=[=($6, $0)], joinType=[left])",
		 "children":[
			{
				"expr": "JoinPartition(condition=[=($6, $0)], joinType=[left])",
				"children": [
					{
						"expr": "PartwiseJoin(condition=[=($3, $1)], joinType=[left])",
						"children": [
							{
								"expr": "JoinPartition(condition=[=($3, $1)], joinType=[left])",
								"children": [
									{
										"expr": "LogicalTableScan(table=[[main, product]])",
										"children": ""
									},
									{
										"expr": "LogicalTableScan(table=[[main, client]])",
										"children": ""
									}
								]
							}
						]
					},
					{
						"expr": "LogicalTableScan(table=[[main, preference]])",
						"children": ""
					}
				]
			}
		]
	}
	)raw";

	std::istringstream inputcmp(jsonCompare);
	boost::property_tree::ptree p_tree_cmp;
	boost::property_tree::read_json(inputcmp, p_tree_cmp);

	ASSERT_EQ(p_tree, p_tree_cmp);
}

TEST_F(PhysicalPlanGeneratorTest, transform_json_tree_simple_window_function)
{
	//	Query
	// 	select product_name, avg(id_client) over (partition by product_name) from product
	//
	//  Optimized Plan
	//	LogicalProject(product_name=[$1], EXPR$1=[/(CASE(>($2, 0), $3, null:BIGINT), $2)])
	//		LogicalWindow(window#0=[window(partition {1} aggs [COUNT($0), $SUM0($0)])])
	//			BindableTableScan(table=[[main, product]], projects=[[1, 2]], aliases=[[id_client, product_name]])

	std::string logicalPlan =
	R"raw(
	{
		"expr": "LogicalProject(product_name=[$1], EXPR$1=[/(CASE(>($2, 0), $3, null:BIGINT), $2)])",
		"children": [
			{
				"expr": "LogicalWindow(window#0=[window(partition {1} aggs [COUNT($0), $SUM0($0)])])",
				"children": [
					{
						"expr": "BindableTableScan(table=[[main, product]], projects=[[1, 2]], aliases=[[id_client, product_name]])",
						"children": []
					}
				]
			}
		]
	}
	)raw";

	Context context(0, {}, {}, logicalPlan, {});
	ral::batch::tree_processor tree{{}, context.clone(), {}, {}, {}, {}, true};

	std::istringstream input(logicalPlan);
	boost::property_tree::ptree p_tree;
	boost::property_tree::read_json(input, p_tree);
	tree.transform_json_tree(p_tree);

	std::string jsonCompare =
	R"raw(
	{
		"expr": "LogicalProject(product_name=[$1], EXPR$1=[/(CASE(>($2, 0), $3, null:BIGINT), $2)])",
		"children": [
			{
				"expr": "LogicalConcatPartitionsByKeys(window#0=[window(partition {1} aggs [COUNT($0), $SUM0($0)])])",
				"children": [
					{
						"expr": "LogicalComputeWindow(window#0=[window(partition {1} aggs [COUNT($0), $SUM0($0)])])",
						"children": [
							{
								"expr": "LogicalSplitByKeys(window#0=[window(partition {1} aggs [COUNT($0), $SUM0($0)])])",
								"children": [
									{
										"expr" : "LogicalOnlySort(window#0=[window(partition {1} aggs [COUNT($0), $SUM0($0)])])",
										"children" : [
											{
												"expr" : "BindableTableScan(table=[[main, product]], projects=[[1, 2]], aliases=[[id_client, product_name]])",
												"children" : []
											}
										]
									}
								]
							}
						]
					}
				]
			}
		]
	}
	)raw";

	std::istringstream inputcmp(jsonCompare);
	boost::property_tree::ptree p_tree_cmp;
	boost::property_tree::read_json(inputcmp, p_tree_cmp);

	ASSERT_EQ(p_tree, p_tree_cmp);
}

TEST_F(PhysicalPlanGeneratorTest, transform_json_tree_window_function)
{
	//	Query
	// 	SELECT product_name, AVG(id_client) OVER (PARTITION BY product_name ORDER BY id_product DESC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW ) FROM product
	//
	//  Optimized Plan
	//	LogicalProject(product_name=[$2], EXPR$1=[/(CASE(>($3, 0), $4, null:BIGINT), $3)])
	//		LogicalWindow(window#0=[window(partition {2} order by [0 DESC] rows between $3 PRECEDING and CURRENT ROW aggs [COUNT($1), $SUM0($1)])])
	//			LogicalTableScan(table=[[main, product]])

	std::string logicalPlan =
	R"raw(
	{
		"expr": "LogicalProject(product_name=[$2], EXPR$1=[/(CASE(>($3, 0), $4, null:BIGINT), $3)])",
		"children": [
			{
				"expr": "LogicalWindow(window#0=[window(partition {2} order by [0 DESC] rows between $3 PRECEDING and CURRENT ROW aggs [COUNT($1), $SUM0($1)])])",
				"children": [
					{
						"expr": "LogicalTableScan(table=[[main, product]])",
						"children": []
					}
				]
			}
		]
	}
	)raw";

	Context context(0, {}, {}, logicalPlan, {});
	ral::batch::tree_processor tree{{}, context.clone(), {}, {}, {}, {}, true};

	std::istringstream input(logicalPlan);
	boost::property_tree::ptree p_tree;
	boost::property_tree::read_json(input, p_tree);
	tree.transform_json_tree(p_tree);

	std::string jsonCompare =
	R"raw(
	{
		"expr": "LogicalProject(product_name=[$2], EXPR$1=[/(CASE(>($3, 0), $4, null:BIGINT), $3)])",
		"children":	[
			{
				"expr": "LogicalConcatPartitionsByKeys(window#0=[window(partition {2} order by [0 DESC] rows between $3 PRECEDING and CURRENT ROW aggs [COUNT($1), $SUM0($1)])])",
				"children": [
					{
						"expr": "LogicalComputeWindow(window#0=[window(partition {2} order by [0 DESC] rows between $3 PRECEDING and CURRENT ROW aggs [COUNT($1), $SUM0($1)])])",
						"children": [
							{
								"expr": "LogicalOnlySort(window#0=[window(partition {2} order by [0 DESC] rows between $3 PRECEDING and CURRENT ROW aggs [COUNT($1), $SUM0($1)])])",
								"children": [
									{
										"expr": "LogicalSplitByKeys(window#0=[window(partition {2} order by [0 DESC] rows between $3 PRECEDING and CURRENT ROW aggs [COUNT($1), $SUM0($1)])])",
										"children": [
											{
												"expr": "LogicalOnlySort(window#0=[window(partition {2} order by [0 DESC] rows between $3 PRECEDING and CURRENT ROW aggs [COUNT($1), $SUM0($1)])])",
												"children": [
													{
														"expr": "LogicalTableScan(table=[[main, product]])",
														"children": []
													}
												]
											}
										]
									}
								]
							}
						]
					}
				]
			}
		]
	}
	)raw";

	std::istringstream inputcmp(jsonCompare);
	boost::property_tree::ptree p_tree_cmp;
	boost::property_tree::read_json(inputcmp, p_tree_cmp);

	ASSERT_EQ(p_tree, p_tree_cmp);
}

TEST_F(PhysicalPlanGeneratorTest, transform_json_tree_window_function_unsupported_throw_exception)
{
	//	Query
	// 	SELECT product_name, AVG(id_client) OVER (PARTITION BY product_name ORDER BY id_product DESC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW ) FROM product
	//
	//  Optimized Plan
	//	LogicalProject(product_name=[$2], EXPR$1=[/(CASE(>($3, 0), $4, null:BIGINT), $3)])
	//		LogicalWindow(window#0=[window(partition {2} order by [0 DESC] rows between $3 PRECEDING and CURRENT ROW aggs [COUNT($1), $SUM0($1)])])
	//			LogicalTableScan(table=[[main, product]])

	try{
		std::string logicalPlan =
		R"raw(
		{
			"expr": "LogicalProject(product_name=[$2], EXPR$1=[/(CASE(>($3, 0), $4, null:BIGINT), $3)])",
			"children": [
				{
					"expr": "LogicalWindow(window#0=[window(partition {2} order by [0 DESC] rows between $3 PRECEDING and CURRENT ROW aggs [COUNT($1), $SUM0($1)])])",
					"children": [
						{
							"expr": "LogicalTableScan(table=[[main, product]])",
							"children": []
						}
					]
				}
			]
		}
		)raw";

		Context context(0, {}, {}, logicalPlan, {});
		ral::batch::tree_processor tree{{}, context.clone(), {}, {}, {}, {}, true};

		tree.build_batch_graph(logicalPlan);

		FAIL();
	} catch(const std::exception& e) {
		SUCCEED();
	}
}
