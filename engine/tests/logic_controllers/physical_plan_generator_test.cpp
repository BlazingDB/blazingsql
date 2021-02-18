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

// Creates a single node Context
std::shared_ptr<Context> make_single_context(std::string logicalPlan) {
	Node master("self");
	std::vector<Node> contextNodes;
	contextNodes.push_back(master);
	std::map<std::string, std::string> config_options;

	std::shared_ptr<Context> context = std::make_shared<Context>(0, contextNodes, contextNodes[0], logicalPlan, config_options);
	return context;
}

// All test using `wf_` are related to window functions
TEST_F(PhysicalPlanGeneratorTest, wf_one_patition_by_single_node)
{
	//	Query
	// 	select product_name, min(id_client) over (partition by product_name) min_ids from product
	//
	//  Optimized Plan
	//	LogicalProject(product_name=[$1], min_ids=[MIN($0) OVER (PARTITION BY $1)])
	//			BindableTableScan(table=[[main, product]], projects=[[0, 1]], aliases=[[min_ids, id_client, product_name]])

	std::string logicalPlan =
	R"raw(
	{
		"expr": "LogicalProject(product_name=[$1], min_ids=[MIN($0) OVER (PARTITION BY $1)])",
		"children": [
			{
				"expr": "BindableTableScan(table=[[main, product]], projects=[[0, 1]], aliases=[[min_ids, id_client, product_name]])",
				"children": []			
			}
		]
	}
	)raw";

	std::shared_ptr<Context> context = make_single_context(logicalPlan);
	ral::batch::tree_processor tree{{}, context->clone(), {}, {}, {}, {}, true};

	std::istringstream input(logicalPlan);
	boost::property_tree::ptree p_tree;
	boost::property_tree::read_json(input, p_tree);
	tree.transform_json_tree(p_tree);

	std::string jsonCompare =
	R"raw(
	{
		"expr": "LogicalProject(product_name=[$1], min_ids=[MIN($0) OVER (PARTITION BY $1)])",
		"children": [
			{
				"expr": "LogicalComputeWindow(product_name=[$1], min_ids=[MIN($0) OVER (PARTITION BY $1)])",
				"children": [
					{
						"expr": "LogicalMerge(product_name=[$1], min_ids=[MIN($0) OVER (PARTITION BY $1)])",
						"children": [
							{
								"expr": "LogicalSingleNodePartition(product_name=[$1], min_ids=[MIN($0) OVER (PARTITION BY $1)])",
								"children": [
									{
										"expr" : "Logical_SortAndSample(product_name=[$1], min_ids=[MIN($0) OVER (PARTITION BY $1)])",
										"children" : [
											{
												"expr" : "BindableTableScan(table=[[main, product]], projects=[[0, 1]], aliases=[[min_ids, id_client, product_name]])",
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

TEST_F(PhysicalPlanGeneratorTest, wf_multiple_patition_by_single_node)
{
	//	Query
	// 	select product_name, min(id_client) over (partition by product_name, product_region) min_ids from product
	//
	//  Optimized Plan
	//	LogicalProject(product_name=[$1], min_ids=[MIN($0) OVER (PARTITION BY $1, $2)])
	//			BindableTableScan(table=[[main, product]], projects=[[0, 1, 2]], aliases=[[min_ids, id_client, product_name, product_region]])

	std::string logicalPlan =
	R"raw(
	{
		"expr": "LogicalProject(product_name=[$1], min_ids=[MIN($0) OVER (PARTITION BY $1, $2)])",
		"children": [
			{
				"expr": "BindableTableScan(table=[[main, product]], projects=[[0, 1, 2]], aliases=[[min_ids, id_client, product_name, product_region]])",
				"children": []			
			}
		]
	}
	)raw";

	std::shared_ptr<Context> context = make_single_context(logicalPlan);
	ral::batch::tree_processor tree{{}, context->clone(), {}, {}, {}, {}, true};

	std::istringstream input(logicalPlan);
	boost::property_tree::ptree p_tree;
	boost::property_tree::read_json(input, p_tree);
	tree.transform_json_tree(p_tree);

	std::string jsonCompare =
	R"raw(
	{
		"expr": "LogicalProject(product_name=[$1], min_ids=[MIN($0) OVER (PARTITION BY $1, $2)])",
		"children": [
			{
				"expr": "LogicalComputeWindow(product_name=[$1], min_ids=[MIN($0) OVER (PARTITION BY $1, $2)])",
				"children": [
					{
						"expr": "LogicalMerge(product_name=[$1], min_ids=[MIN($0) OVER (PARTITION BY $1, $2)])",
						"children": [
							{
								"expr": "LogicalSingleNodePartition(product_name=[$1], min_ids=[MIN($0) OVER (PARTITION BY $1, $2)])",
								"children": [
									{
										"expr" : "Logical_SortAndSample(product_name=[$1], min_ids=[MIN($0) OVER (PARTITION BY $1, $2)])",
										"children" : [
											{
												"expr" : "BindableTableScan(table=[[main, product]], projects=[[0, 1, 2]], aliases=[[min_ids, id_client, product_name, product_region]])",
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

TEST_F(PhysicalPlanGeneratorTest, wf_one_patition_by_one_order_by_single_node)
{
	//	Query
	//	select product_name, max(id_client) over (partition by product_name order by product_region) max_ids from product
	//
	//	Optimized Plan
	//	LogicalProject(product_name=[$1], max_ids=[MAX($0) OVER (PARTITION BY $1 ORDER BY $2)])
	//			BindableTableScan(table=[[main, product]], projects=[[0, 1, 2]], aliases=[[max_ids, id_client, product_name, product_region]])

	std::string logicalPlan =
	R"raw(
	{
		"expr": "LogicalProject(product_name=[$1], max_ids=[MAX($0) OVER (PARTITION BY $1 ORDER BY $2)])",
		"children": [
			{
				"expr": "BindableTableScan(table=[[main, product]], projects=[[0, 1, 2]], aliases=[[max_ids, id_client, product_name, product_region]])",
				"children": []			
			}
		]
	}
	)raw";

	std::shared_ptr<Context> context = make_single_context(logicalPlan);
	ral::batch::tree_processor tree{{}, context->clone(), {}, {}, {}, {}, true};

	std::istringstream input(logicalPlan);
	boost::property_tree::ptree p_tree;
	boost::property_tree::read_json(input, p_tree);
	tree.transform_json_tree(p_tree);

	std::string jsonCompare =
	R"raw(
	{
		"expr": "LogicalProject(product_name=[$1], max_ids=[MAX($0) OVER (PARTITION BY $1 ORDER BY $2)])",
		"children": [
			{
				"expr": "LogicalComputeWindow(product_name=[$1], max_ids=[MAX($0) OVER (PARTITION BY $1 ORDER BY $2)])",
				"children": [
					{
						"expr": "LogicalMerge(product_name=[$1], max_ids=[MAX($0) OVER (PARTITION BY $1 ORDER BY $2)])",
						"children": [
							{
								"expr": "LogicalSingleNodePartition(product_name=[$1], max_ids=[MAX($0) OVER (PARTITION BY $1 ORDER BY $2)])",
								"children": [
									{
										"expr" : "Logical_SortAndSample(product_name=[$1], max_ids=[MAX($0) OVER (PARTITION BY $1 ORDER BY $2)])",
										"children" : [
											{
												"expr" : "BindableTableScan(table=[[main, product]], projects=[[0, 1, 2]], aliases=[[max_ids, id_client, product_name, product_region]])",
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

TEST_F(PhysicalPlanGeneratorTest, wf_multiple_patition_by_multiple_order_by_single_node)
{
	//	Query
	// 	select product_name, max(id_client) over (partition by product_name, product_region order by product_country, product_price desc) max_ids from product
	//
	//	Optimized Plan
	//	LogicalProject(product_name=[$1], max_ids=[MAX($0) OVER (PARTITION BY $1, $2 ORDER BY $3, $4 DESC)])
	//			BindableTableScan(table=[[main, product]], projects=[[0, 1, 2, 3, 4]], aliases=[[max_ids, id_client, product_name, product_region, product_country, product_price]])

	std::string logicalPlan =
	R"raw(
	{
		"expr": "LogicalProject(product_name=[$1], max_ids=[MAX($0) OVER (PARTITION BY $1, $2 ORDER BY $3, $4 DESC)])",
		"children": [
			{
				"expr": "BindableTableScan(table=[[main, product]], projects=[[0, 1, 2, 3, 4]], aliases=[[max_ids, id_client, product_name, product_region, product_country, product_price]])",
				"children": []			
			}
		]
	}
	)raw";

	std::shared_ptr<Context> context = make_single_context(logicalPlan);
	ral::batch::tree_processor tree{{}, context->clone(), {}, {}, {}, {}, true};

	std::istringstream input(logicalPlan);
	boost::property_tree::ptree p_tree;
	boost::property_tree::read_json(input, p_tree);
	tree.transform_json_tree(p_tree);

	std::string jsonCompare =
	R"raw(
	{
		"expr": "LogicalProject(product_name=[$1], max_ids=[MAX($0) OVER (PARTITION BY $1, $2 ORDER BY $3, $4 DESC)])",
		"children": [
			{
				"expr": "LogicalComputeWindow(product_name=[$1], max_ids=[MAX($0) OVER (PARTITION BY $1, $2 ORDER BY $3, $4 DESC)])",
				"children": [
					{
						"expr": "LogicalMerge(product_name=[$1], max_ids=[MAX($0) OVER (PARTITION BY $1, $2 ORDER BY $3, $4 DESC)])",
						"children": [
							{
								"expr": "LogicalSingleNodePartition(product_name=[$1], max_ids=[MAX($0) OVER (PARTITION BY $1, $2 ORDER BY $3, $4 DESC)])",
								"children": [
									{
										"expr" : "Logical_SortAndSample(product_name=[$1], max_ids=[MAX($0) OVER (PARTITION BY $1, $2 ORDER BY $3, $4 DESC)])",
										"children" : [
											{
												"expr" : "BindableTableScan(table=[[main, product]], projects=[[0, 1, 2, 3, 4]], aliases=[[max_ids, id_client, product_name, product_region, product_country, product_price]])",
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

TEST_F(PhysicalPlanGeneratorTest, multiple_equal_over_clauses_single_node)
{
	//	Query
	//	select min(n_nationkey) over (partition by n_regionkey order by n_name) min_keys, 
	//				max(n_nationkey) over (partition by n_regionkey order by n_name) max_keys,
	//				n_name from nation
	//
	//	Optimized Plan
	//	LogicalProject(min_keys=[MIN($0) OVER (PARTITION BY $2 ORDER BY $1)], max_keys=[MAX($0) OVER (PARTITION BY $2 ORDER BY $1)])
	//			LogicalTableScan(table=[[main, nation]])

	std::string logicalPlan =
	R"raw(
	{
		"expr": "LogicalProject(min_keys=[MIN($0) OVER (PARTITION BY $2 ORDER BY $1)], max_keys=[MAX($0) OVER (PARTITION BY $2 ORDER BY $1)])",
		"children": [
			{
				"expr": "BindableTableScan(table=[[main, nation]])",
				"children": []			
			}
		]
	}
	)raw";

	std::shared_ptr<Context> context = make_single_context(logicalPlan);
	ral::batch::tree_processor tree{{}, context->clone(), {}, {}, {}, {}, true};

	std::istringstream input(logicalPlan);
	boost::property_tree::ptree p_tree;
	boost::property_tree::read_json(input, p_tree);
	tree.transform_json_tree(p_tree);

	std::string jsonCompare =
	R"raw(
	{
		"expr": "LogicalProject(min_keys=[MIN($0) OVER (PARTITION BY $2 ORDER BY $1)], max_keys=[MAX($0) OVER (PARTITION BY $2 ORDER BY $1)])",
		"children":	[
			{
				"expr": "LogicalComputeWindow(min_keys=[MIN($0) OVER (PARTITION BY $2 ORDER BY $1)], max_keys=[MAX($0) OVER (PARTITION BY $2 ORDER BY $1)])",
				"children": [
					{
						"expr": "LogicalMerge(min_keys=[MIN($0) OVER (PARTITION BY $2 ORDER BY $1)], max_keys=[MAX($0) OVER (PARTITION BY $2 ORDER BY $1)])",
						"children": [
							{
								"expr": "LogicalSingleNodePartition(min_keys=[MIN($0) OVER (PARTITION BY $2 ORDER BY $1)], max_keys=[MAX($0) OVER (PARTITION BY $2 ORDER BY $1)])",
								"children": [
									{
										"expr": "Logical_SortAndSample(min_keys=[MIN($0) OVER (PARTITION BY $2 ORDER BY $1)], max_keys=[MAX($0) OVER (PARTITION BY $2 ORDER BY $1)])",
										"children": [
											{
												"expr": "BindableTableScan(table=[[main, nation]])",
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
	)raw";

	std::istringstream inputcmp(jsonCompare);
	boost::property_tree::ptree p_tree_cmp;
	boost::property_tree::read_json(inputcmp, p_tree_cmp);

	ASSERT_EQ(p_tree, p_tree_cmp);
}

TEST_F(PhysicalPlanGeneratorTest, multiple_diff_over_clauses_single_node)
{
	//	Query
	//	select min(n_nationkey) over (partition by n_regionkey) min_keys, 
	//				max(n_nationkey) over (partition by n_name) max_keys,
	//				n_name from nation
	//
	//	Optimized Plan
	//	LogicalProject(min_keys=[MIN($0) OVER (PARTITION BY $2)], max_keys=[MAX($0) OVER (PARTITION BY $1)])
	//			LogicalTableScan(table=[[main, nation]])
	try {
		std::string logicalPlan =
		R"raw(
		{
			"expr": "LogicalProject(min_keys=[MIN($0) OVER (PARTITION BY $2)], max_keys=[MAX($0) OVER (PARTITION BY $1)])",
			"children": [
				{
					"expr": "BindableTableScan(table=[[main, nation]])",
					"children": []			
				}
			]
		}
		)raw";

			std::shared_ptr<Context> context = make_single_context(logicalPlan);
			ral::batch::tree_processor tree{{}, context->clone(), {}, {}, {}, {}, true};

			tree.build_batch_graph(logicalPlan);

			FAIL();
	} catch(const std::exception& e) {
		SUCCEED();
	}
}

/*
// TODO: cordova add more unit_test to ROW bounding clauses when they are added in WindowFunctionTest
TEST_F(PhysicalPlanGeneratorTest, simple_window_function_distributed)
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
				"expr": "LogicalComputeWindow(window#0=[window(partition {2} order by [0 DESC] rows between $3 PRECEDING and CURRENT ROW aggs [COUNT($1), $SUM0($1)])])",
				"children": [
					{
						"expr": "LogicalMerge(window#0=[window(partition {2} order by [0 DESC] rows between $3 PRECEDING and CURRENT ROW aggs [COUNT($1), $SUM0($1)])])",
						"children": [
							{
								"expr": "LogicalPartition(window#0=[window(partition {2} order by [0 DESC] rows between $3 PRECEDING and CURRENT ROW aggs [COUNT($1), $SUM0($1)])])",
								"children": [
									{
										"expr": "Logical_SortAndSample(window#0=[window(partition {2} order by [0 DESC] rows between $3 PRECEDING and CURRENT ROW aggs [COUNT($1), $SUM0($1)])])",
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
	)raw";

	std::istringstream inputcmp(jsonCompare);
	boost::property_tree::ptree p_tree_cmp;
	boost::property_tree::read_json(inputcmp, p_tree_cmp);

	ASSERT_EQ(p_tree, p_tree_cmp);
}

TEST_F(PhysicalPlanGeneratorTest, window_function_single_node)
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

	std::shared_ptr<Context> context = make_single_context(logicalPlan);
	ral::batch::tree_processor tree{{}, context->clone(), {}, {}, {}, {}, true};

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
				"expr": "LogicalComputeWindow(window#0=[window(partition {2} order by [0 DESC] rows between $3 PRECEDING and CURRENT ROW aggs [COUNT($1), $SUM0($1)])])",
				"children": [
					{
						"expr": "LogicalMerge(window#0=[window(partition {2} order by [0 DESC] rows between $3 PRECEDING and CURRENT ROW aggs [COUNT($1), $SUM0($1)])])",
						"children": [
							{
								"expr": "LogicalSingleNodePartition(window#0=[window(partition {2} order by [0 DESC] rows between $3 PRECEDING and CURRENT ROW aggs [COUNT($1), $SUM0($1)])])",
								"children": [
									{
										"expr": "Logical_SortAndSample(window#0=[window(partition {2} order by [0 DESC] rows between $3 PRECEDING and CURRENT ROW aggs [COUNT($1), $SUM0($1)])])",
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
	)raw";

	std::istringstream inputcmp(jsonCompare);
	boost::property_tree::ptree p_tree_cmp;
	boost::property_tree::read_json(inputcmp, p_tree_cmp);

	ASSERT_EQ(p_tree, p_tree_cmp);
}


TEST_F(PhysicalPlanGeneratorTest, window_function_distributed)
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
				"expr": "LogicalComputeWindow(window#0=[window(partition {2} order by [0 DESC] rows between $3 PRECEDING and CURRENT ROW aggs [COUNT($1), $SUM0($1)])])",
				"children": [
					{
						"expr": "LogicalMerge(window#0=[window(partition {2} order by [0 DESC] rows between $3 PRECEDING and CURRENT ROW aggs [COUNT($1), $SUM0($1)])])",
						"children": [
							{
								"expr": "LogicalPartition(window#0=[window(partition {2} order by [0 DESC] rows between $3 PRECEDING and CURRENT ROW aggs [COUNT($1), $SUM0($1)])])",
								"children": [
									{
										"expr": "Logical_SortAndSample(window#0=[window(partition {2} order by [0 DESC] rows between $3 PRECEDING and CURRENT ROW aggs [COUNT($1), $SUM0($1)])])",
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
	)raw";

	std::istringstream inputcmp(jsonCompare);
	boost::property_tree::ptree p_tree_cmp;
	boost::property_tree::read_json(inputcmp, p_tree_cmp);

	ASSERT_EQ(p_tree, p_tree_cmp);
}

TEST_F(PhysicalPlanGeneratorTest, window_function_bounded_single_node)
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

		std::shared_ptr<Context> context = make_single_context(logicalPlan);
		ral::batch::tree_processor tree{{}, context->clone(), {}, {}, {}, {}, true};

		tree.build_batch_graph(logicalPlan);

		FAIL();
	} catch(const std::exception& e) {
		SUCCEED();
	}
}
*/