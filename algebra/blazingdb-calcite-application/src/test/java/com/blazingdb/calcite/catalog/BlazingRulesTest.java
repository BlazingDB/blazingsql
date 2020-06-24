package com.blazingdb.calcite.catalog;

import com.blazingdb.calcite.application.CalciteApplication;
import com.blazingdb.calcite.application.RelationalAlgebraGenerator;
import com.blazingdb.calcite.catalog.domain.CatalogColumnDataType;
import com.blazingdb.calcite.catalog.domain.CatalogColumnImpl;
import com.blazingdb.calcite.catalog.domain.CatalogDatabaseImpl;
import com.blazingdb.calcite.catalog.domain.CatalogTableImpl;
import com.blazingdb.calcite.catalog.repository.DatabaseRepository;
import com.blazingdb.calcite.schema.BlazingSchema;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterTableScanRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectTableScanRule;
import org.apache.calcite.schema.Table;

import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import java.util.AbstractMap;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import org.testng.asserts.SoftAssert;

public class BlazingRulesTest {
	private static SessionFactory sessionFactory = null;

	@BeforeMethod
	public void
	setUp() throws Exception {
		CalciteApplication.executeUpdate();
		repo = new DatabaseRepository();
		sessionFactory = new Configuration().configure().buildSessionFactory();
		db = new CatalogDatabaseImpl("main");
	}

	@AfterMethod
	public void
	tearDown() throws Exception {
		repo.dropDatabase(db);
		sessionFactory.close();
	}

	DatabaseRepository repo;
	CatalogDatabaseImpl db;
	private Long dbId = -1L;

	public void
	createTableSchemas() throws Exception {
		System.out.println(
			"=============================== GENERATE RELATIONAL ALGEBRA WITH DIFFERENT RULES TEST ====================================");
		final long startTime = System.currentTimeMillis();

		repo.createDatabase(db);
		dbId = db.getId();

		Map<String, List<Entry<String, CatalogColumnDataType>>> map = new HashMap<>();
		map.put("customer",
			Arrays.asList(new SimpleEntry<>("c_custkey", CatalogColumnDataType.INT32),
				new SimpleEntry<>("c_name", CatalogColumnDataType.STRING),
				new SimpleEntry<>("c_address", CatalogColumnDataType.STRING),
				new SimpleEntry<>("c_nationkey", CatalogColumnDataType.INT32),
				new SimpleEntry<>("c_phone", CatalogColumnDataType.STRING),
				new SimpleEntry<>("c_acctbal", CatalogColumnDataType.FLOAT64),
				new SimpleEntry<>("c_mktsegment", CatalogColumnDataType.STRING),
				new SimpleEntry<>("c_comment", CatalogColumnDataType.STRING)));
		map.put("region",
			Arrays.asList(new SimpleEntry<>("r_regionkey", CatalogColumnDataType.INT32),
				new SimpleEntry<>("r_name", CatalogColumnDataType.STRING),
				new SimpleEntry<>("r_comment", CatalogColumnDataType.STRING)));
		map.put("nation",
			Arrays.asList(new SimpleEntry<>("n_nationkey", CatalogColumnDataType.INT32),
				new SimpleEntry<>("n_name", CatalogColumnDataType.STRING),
				new SimpleEntry<>("n_regionkey", CatalogColumnDataType.INT32),
				new SimpleEntry<>("n_comment", CatalogColumnDataType.STRING)));
		map.put("orders",
			Arrays.asList(new SimpleEntry<>("o_orderkey", CatalogColumnDataType.INT64),
				new SimpleEntry<>("o_custkey", CatalogColumnDataType.INT32),
				new SimpleEntry<>("o_orderstatus", CatalogColumnDataType.STRING),
				new SimpleEntry<>("o_totalprice", CatalogColumnDataType.FLOAT64),
				new SimpleEntry<>("o_orderdate", CatalogColumnDataType.TIMESTAMP_MILLISECONDS),
				new SimpleEntry<>("o_orderpriority", CatalogColumnDataType.STRING),
				new SimpleEntry<>("o_clerk", CatalogColumnDataType.STRING),
				new SimpleEntry<>("o_shippriority", CatalogColumnDataType.STRING),
				new SimpleEntry<>("o_comment", CatalogColumnDataType.STRING)));
		map.put("supplier",
			Arrays.asList(new SimpleEntry<>("s_suppkey", CatalogColumnDataType.INT64),
				new SimpleEntry<>("s_name", CatalogColumnDataType.STRING),
				new SimpleEntry<>("s_address", CatalogColumnDataType.STRING),
				new SimpleEntry<>("s_nationkey", CatalogColumnDataType.INT32),
				new SimpleEntry<>("s_phone", CatalogColumnDataType.STRING),
				new SimpleEntry<>("s_acctbal", CatalogColumnDataType.FLOAT64),
				new SimpleEntry<>("s_comment", CatalogColumnDataType.STRING)));
		map.put("lineitem",
			Arrays.asList(new SimpleEntry<>("l_orderkey", CatalogColumnDataType.INT64),
				new SimpleEntry<>("l_partkey", CatalogColumnDataType.INT64),
				new SimpleEntry<>("l_suppkey", CatalogColumnDataType.INT64),
				new SimpleEntry<>("l_linenumber", CatalogColumnDataType.INT32),
				new SimpleEntry<>("l_quantity", CatalogColumnDataType.FLOAT64),
				new SimpleEntry<>("l_extendedprice", CatalogColumnDataType.FLOAT64),
				new SimpleEntry<>("l_discount", CatalogColumnDataType.FLOAT64),
				new SimpleEntry<>("l_tax", CatalogColumnDataType.FLOAT64),
				new SimpleEntry<>("l_returnflag", CatalogColumnDataType.STRING),
				new SimpleEntry<>("l_linestatus", CatalogColumnDataType.STRING),
				new SimpleEntry<>("l_shipdate", CatalogColumnDataType.TIMESTAMP_MILLISECONDS),
				new SimpleEntry<>("l_commitdate", CatalogColumnDataType.TIMESTAMP_MILLISECONDS),
				new SimpleEntry<>("l_receiptdate", CatalogColumnDataType.TIMESTAMP_MILLISECONDS),
				new SimpleEntry<>("l_shipinstruct", CatalogColumnDataType.STRING),
				new SimpleEntry<>("l_shipmode", CatalogColumnDataType.STRING),
				new SimpleEntry<>("l_comment", CatalogColumnDataType.STRING)));
		map.put("part",
			Arrays.asList(new SimpleEntry<>("p_partkey", CatalogColumnDataType.INT64),
				new SimpleEntry<>("p_name", CatalogColumnDataType.STRING),
				new SimpleEntry<>("p_mfgr", CatalogColumnDataType.STRING),
				new SimpleEntry<>("p_brand", CatalogColumnDataType.STRING),
				new SimpleEntry<>("p_type", CatalogColumnDataType.STRING),
				new SimpleEntry<>("p_size", CatalogColumnDataType.INT64),
				new SimpleEntry<>("p_container", CatalogColumnDataType.STRING),
				new SimpleEntry<>("p_retailprice", CatalogColumnDataType.FLOAT32),
				new SimpleEntry<>("p_comment", CatalogColumnDataType.STRING)));
		map.put("partsupp",
			Arrays.asList(new SimpleEntry<>("ps_partkey", CatalogColumnDataType.INT64),
				new SimpleEntry<>("ps_suppkey", CatalogColumnDataType.INT64),
				new SimpleEntry<>("ps_availqty", CatalogColumnDataType.INT64),
				new SimpleEntry<>("ps_supplycost", CatalogColumnDataType.FLOAT32),
				new SimpleEntry<>("ps_comment", CatalogColumnDataType.STRING)));

		for(Map.Entry<String, List<Entry<String, CatalogColumnDataType>>> entry : map.entrySet()) {
			List<CatalogColumnImpl> columns = new ArrayList<CatalogColumnImpl>();

			int order_value = 0;
			for(Entry<String, CatalogColumnDataType> field : entry.getValue()) {
				columns.add(new CatalogColumnImpl(field.getKey(), field.getValue(), order_value++));
			}

			CatalogTableImpl table = new CatalogTableImpl(entry.getKey(), db, columns);

			db.addTable(table);
			repo.updateDatabase(db);
		}

		final long endTime = System.currentTimeMillis();
		System.out.println("Total execution time: " + (endTime - startTime));
	}

	public void
	checkTable(BlazingSchema schema, String table_name) throws Exception {
		Table tableTemp1 = schema.getTable(table_name);
		if(tableTemp1 == null) {
			System.out.println("table " + table_name + " NOT found");
			throw new Exception();
		} else {
			System.out.println("table found");
		}
	}

	@Test()
	public void
	generateSQLTest() throws Exception {
		createTableSchemas();

		db = repo.getDatabase(dbId);

		BlazingSchema schema = new BlazingSchema(db);

		checkTable(schema, "customer");
		checkTable(schema, "orders");

		RelationalAlgebraGenerator algebraGen = new RelationalAlgebraGenerator(schema);

		List<List<RelOptRule>> rulesSet = new ArrayList<List<RelOptRule>>();

		List<RelOptRule> rules1 = Arrays.asList(ProjectFilterTransposeRule.INSTANCE,
			FilterJoinRule.JoinConditionPushRule.FILTER_ON_JOIN,
			FilterJoinRule.JoinConditionPushRule.JOIN,
			ProjectMergeRule.INSTANCE,
			FilterMergeRule.INSTANCE,
			ProjectJoinTransposeRule.INSTANCE,
			ProjectTableScanRule.INSTANCE);

		rulesSet.add(rules1);

		List<RelOptRule> rules2 = Arrays.asList(FilterJoinRule.JoinConditionPushRule.FILTER_ON_JOIN,
			FilterJoinRule.JoinConditionPushRule.JOIN,
			ProjectMergeRule.INSTANCE,
			FilterMergeRule.INSTANCE,
			ProjectJoinTransposeRule.INSTANCE,
			ProjectFilterTransposeRule.INSTANCE,
			ProjectTableScanRule.INSTANCE);

		rulesSet.add(rules2);

		for(List<RelOptRule> rules : rulesSet) {
			System.out.println("<*****************************************************************************>");

			String sql =
				"select c_custkey from `customer` inner join `orders` on c_custkey = o_custkey where c_custkey < 1000";
			RelNode nonOptimizedPlan = algebraGen.getNonOptimizedRelationalAlgebra(sql);
			System.out.println("non optimized\n");
			System.out.println(RelOptUtil.toString(nonOptimizedPlan) + "\n");

			for(int I = 0; I < rules.size(); I++) {
				algebraGen.setRules(rules.subList(0, I + 1));

				RelNode optimizedPlan = algebraGen.getOptimizedRelationalAlgebra(nonOptimizedPlan);

				System.out.println("optimized by rule: " + rules.get(I).getClass().getName() + "\n");
				System.out.println(RelOptUtil.toString(optimizedPlan) + "\n");
			}
		}
	}

	//TPCH queries
	List<Entry<String, String>> tpch_queries = Arrays.asList(
		new AbstractMap.SimpleEntry<String, String>("tpch01", "SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base_price, sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, avg(l_quantity) AS avg_qty, avg(l_extendedprice) AS avg_price, avg(l_discount) AS avg_disc, count(*) AS count_order FROM lineitem WHERE l_shipdate <= cast('1998-09-02' AS date) GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus"),
		new AbstractMap.SimpleEntry<String, String>("tpch02", "SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment FROM part, supplier, partsupp, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = 15 AND p_type LIKE '%BRASS' AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'EUROPE' AND ps_supplycost = ( SELECT min(ps_supplycost) FROM partsupp, supplier, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'EUROPE') ORDER BY s_acctbal DESC, n_name, s_name, p_partkey LIMIT 100"),
		new AbstractMap.SimpleEntry<String, String>("tpch03", "SELECT l_orderkey, sum(l_extendedprice * (1 - l_discount)) AS revenue, o_orderdate, o_shippriority FROM customer, orders, lineitem WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < cast('1995-03-15' AS date) AND l_shipdate > cast('1995-03-15' AS date) GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue DESC, o_orderdate LIMIT 10"),
		new AbstractMap.SimpleEntry<String, String>("tpch04", "SELECT o_orderpriority, count(*) AS order_count FROM orders WHERE o_orderdate >= cast('1993-07-01' AS date) AND o_orderdate < cast('1993-10-01' AS date) AND EXISTS ( SELECT * FROM lineitem WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate) GROUP BY o_orderpriority ORDER BY o_orderpriority"),
		new AbstractMap.SimpleEntry<String, String>("tpch05", "SELECT n_name, sum(l_extendedprice * (1 - l_discount)) AS revenue FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'ASIA' AND o_orderdate >= cast('1994-01-01' AS date) AND o_orderdate < cast('1995-01-01' AS date) GROUP BY n_name ORDER BY revenue DESC"),
		new AbstractMap.SimpleEntry<String, String>("tpch06", "SELECT sum(l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_shipdate >= cast('1994-01-01' AS date) AND l_shipdate < cast('1995-01-01' AS date) AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24"),
		new AbstractMap.SimpleEntry<String, String>("tpch07", "SELECT supp_nation, cust_nation, l_year, sum(volume) AS revenue FROM ( SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation, extract(year FROM l_shipdate) AS l_year, l_extendedprice * (1 - l_discount) AS volume FROM supplier, lineitem, orders, customer, nation n1, nation n2 WHERE s_suppkey = l_suppkey AND o_orderkey = l_orderkey AND c_custkey = o_custkey AND s_nationkey = n1.n_nationkey AND c_nationkey = n2.n_nationkey AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY') OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')) AND l_shipdate BETWEEN cast('1995-01-01' AS date) AND cast('1996-12-31' AS date)) AS shipping GROUP BY supp_nation, cust_nation, l_year ORDER BY supp_nation, cust_nation, l_year"),
		new AbstractMap.SimpleEntry<String, String>("tpch08", "SELECT o_year, sum( CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / sum(volume) AS mkt_share FROM ( SELECT extract(year FROM o_orderdate) AS o_year, l_extendedprice * (1 - l_discount) AS volume, n2.n_name AS nation FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey AND l_orderkey = o_orderkey AND o_custkey = c_custkey AND c_nationkey = n1.n_nationkey AND n1.n_regionkey = r_regionkey AND r_name = 'AMERICA' AND s_nationkey = n2.n_nationkey AND o_orderdate BETWEEN cast('1995-01-01' AS date) AND cast('1996-12-31' AS date) AND p_type = 'ECONOMY ANODIZED STEEL') AS all_nations GROUP BY o_year ORDER BY o_year"),
		new AbstractMap.SimpleEntry<String, String>("tpch09", "SELECT nation, o_year, sum(amount) AS sum_profit FROM ( SELECT n_name AS nation, extract(year FROM o_orderdate) AS o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount FROM part, supplier, lineitem, partsupp, orders, nation WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey AND ps_partkey = l_partkey AND p_partkey = l_partkey AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey AND p_name LIKE '%green%') AS profit GROUP BY nation, o_year ORDER BY nation, o_year DESC"),
		new AbstractMap.SimpleEntry<String, String>("tpch10", "SELECT c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) AS revenue, c_acctbal, n_name, c_address, c_phone, c_comment FROM customer, orders, lineitem, nation WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate >= cast('1993-10-01' AS date) AND o_orderdate < cast('1994-01-01' AS date) AND l_returnflag = 'R' AND c_nationkey = n_nationkey GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment ORDER BY revenue DESC LIMIT 20"),
		new AbstractMap.SimpleEntry<String, String>("tpch11", "SELECT ps_partkey, sum(ps_supplycost * ps_availqty) AS  valuep FROM partsupp, supplier, nation WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY' GROUP BY ps_partkey HAVING sum(ps_supplycost * ps_availqty) > ( SELECT sum(ps_supplycost * ps_availqty) * 0.0001000000 FROM partsupp, supplier, nation WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY') ORDER BY valuep DESC"),
		new AbstractMap.SimpleEntry<String, String>("tpch12", "SELECT l_shipmode, sum( CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS high_line_count, sum( CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) AS low_line_count FROM orders, lineitem WHERE o_orderkey = l_orderkey AND l_shipmode IN ('MAIL', 'SHIP') AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate AND l_receiptdate >= cast('1994-01-01' AS date) AND l_receiptdate < cast('1995-01-01' AS date) GROUP BY l_shipmode ORDER BY l_shipmode"),
		new AbstractMap.SimpleEntry<String, String>("tpch13", "SELECT c_count, count(*) AS custdist FROM ( SELECT c_custkey, count(o_orderkey) FROM customer LEFT OUTER JOIN orders ON c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%' GROUP BY c_custkey) AS c_orders (c_custkey, c_count) GROUP BY c_count ORDER BY custdist DESC, c_count DESC"),
		new AbstractMap.SimpleEntry<String, String>("tpch14", "SELECT 100.00 * sum( CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue FROM lineitem, part WHERE l_partkey = p_partkey AND l_shipdate >= date '1995-09-01' AND l_shipdate < cast('1995-10-01' AS date)"),
		new AbstractMap.SimpleEntry<String, String>("tpch15", "SELECT s_suppkey, s_name, s_address, s_phone, total_revenue FROM supplier, ( SELECT l_suppkey AS supplier_no, sum(l_extendedprice * (1 - l_discount)) AS total_revenue FROM lineitem WHERE l_shipdate >= cast('1996-01-01' AS date) AND l_shipdate < cast('1996-04-01' AS date) GROUP BY l_suppkey) revenue0 WHERE s_suppkey = supplier_no AND total_revenue = ( SELECT max(total_revenue) FROM ( SELECT l_suppkey AS supplier_no, sum(l_extendedprice * (1 - l_discount)) AS total_revenue FROM lineitem WHERE l_shipdate >= cast('1996-01-01' AS date) AND l_shipdate < cast('1996-04-01' AS date) GROUP BY l_suppkey) revenue1) ORDER BY s_suppkey"),
		new AbstractMap.SimpleEntry<String, String>("tpch16", "SELECT p_brand, p_type, p_size, count(DISTINCT ps_suppkey) AS supplier_cnt FROM partsupp, part WHERE p_partkey = ps_partkey AND p_brand <> 'Brand#45' AND p_type NOT LIKE 'MEDIUM POLISHED%' AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9) AND ps_suppkey NOT IN ( SELECT s_suppkey FROM supplier WHERE s_comment LIKE '%Customer%Complaints%') GROUP BY p_brand, p_type, p_size ORDER BY supplier_cnt DESC, p_brand, p_type, p_size"),
		new AbstractMap.SimpleEntry<String, String>("tpch17", "SELECT sum(l_extendedprice) / 7.0 AS avg_yearly FROM lineitem, part WHERE p_partkey = l_partkey AND p_brand = 'Brand#23' AND p_container = 'MED BOX' AND l_quantity < ( SELECT 0.2 * avg(l_quantity) FROM lineitem WHERE l_partkey = p_partkey)"),
		new AbstractMap.SimpleEntry<String, String>("tpch18", "SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) FROM customer, orders, lineitem WHERE o_orderkey IN ( SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING sum(l_quantity) > 300) AND c_custkey = o_custkey AND o_orderkey = l_orderkey GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice ORDER BY o_totalprice DESC, o_orderdate LIMIT 100"),
		new AbstractMap.SimpleEntry<String, String>("tpch19", "SELECT sum(l_extendedprice * (1 - l_discount)) AS revenue FROM lineitem, part WHERE (p_partkey = l_partkey AND p_brand = 'Brand#12' AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND l_quantity >= 1 AND l_quantity <= 1 + 10 AND p_size BETWEEN 1 AND 5 AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON') OR (p_partkey = l_partkey AND p_brand = 'Brand#23' AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND l_quantity >= 10 AND l_quantity <= 10 + 10 AND p_size BETWEEN 1 AND 10 AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON') OR (p_partkey = l_partkey AND p_brand = 'Brand#34' AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND l_quantity >= 20 AND l_quantity <= 20 + 10 AND p_size BETWEEN 1 AND 15 AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON')"),
		new AbstractMap.SimpleEntry<String, String>("tpch20", "SELECT s_name, s_address FROM supplier, nation WHERE s_suppkey IN ( SELECT ps_suppkey FROM partsupp WHERE ps_partkey IN ( SELECT p_partkey FROM part WHERE p_name LIKE 'forest%') AND ps_availqty > ( SELECT 0.5 * sum(l_quantity) FROM lineitem WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey AND l_shipdate >= cast('1994-01-01' AS date) AND l_shipdate < cast('1995-01-01' AS date))) AND s_nationkey = n_nationkey AND n_name = 'CANADA' ORDER BY s_name"),
		new AbstractMap.SimpleEntry<String, String>("tpch21", "SELECT s_name, count(*) AS numwait FROM supplier, lineitem l1, orders, nation WHERE s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey AND o_orderstatus = 'F' AND l1.l_receiptdate > l1.l_commitdate AND EXISTS ( SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey) AND NOT EXISTS ( SELECT * FROM lineitem l3 WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey AND l3.l_receiptdate > l3.l_commitdate) AND s_nationkey = n_nationkey AND n_name = 'SAUDI ARABIA' GROUP BY s_name ORDER BY numwait DESC, s_name LIMIT 100"),
		new AbstractMap.SimpleEntry<String, String>("tpch22", "SELECT cntrycode, count(*) AS numcust, sum(c_acctbal) AS totacctbal FROM ( SELECT substring(c_phone FROM 1 FOR 2) AS cntrycode, c_acctbal FROM customer WHERE substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17') AND c_acctbal > ( SELECT avg(c_acctbal) FROM customer WHERE c_acctbal > 0.00 AND substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')) AND NOT EXISTS ( SELECT * FROM orders WHERE o_custkey = c_custkey)) AS custsale GROUP BY cntrycode ORDER BY cntrycode")
	);

	String reference_filename = "tpch-reference.txt";

	// Enable this unit test for updating the reference optimized plans for all TPCH queries
	@Test(enabled = false)
	public void
	generateLogicalPlanTest() throws Exception {

		createTableSchemas();
		db = repo.getDatabase(dbId);

		BlazingSchema schema = new BlazingSchema(db);
		RelationalAlgebraGenerator algebraGen = new RelationalAlgebraGenerator(schema);

		FileOutputStream file = new FileOutputStream(reference_filename);
		ObjectOutputStream out = new ObjectOutputStream(file);

		for (Entry<String, String> entry : tpch_queries)
		{
			String sql = entry.getValue();
			RelNode nonOptimizedPlan = algebraGen.getNonOptimizedRelationalAlgebra(sql);
			RelNode optimizedPlan = algebraGen.getOptimizedRelationalAlgebra(nonOptimizedPlan);
			String logicalPlan = RelOptUtil.toString(optimizedPlan);

			out.writeObject(logicalPlan);
		}

		out.close();
		file.close();
	}

	// When enabled, this unit test compares for all TPCH queries, the current optimized logical plans versus the last reference plans
	@Test(enabled = true)
	public void
	checkLogicalPlanTest() throws Exception {

		createTableSchemas();
		db = repo.getDatabase(dbId);

		BlazingSchema schema = new BlazingSchema(db);
		RelationalAlgebraGenerator algebraGen = new RelationalAlgebraGenerator(schema);

		FileInputStream file = new FileInputStream(reference_filename);
		ObjectInputStream in = new ObjectInputStream(file);

		SoftAssert softAssert = new SoftAssert();

		for (Entry<String, String> entry : tpch_queries)
		{
			String sql = entry.getValue();
			RelNode nonOptimizedPlan = algebraGen.getNonOptimizedRelationalAlgebra(sql);
			RelNode optimizedPlan = algebraGen.getOptimizedRelationalAlgebra(nonOptimizedPlan);
			String logicalPlan = RelOptUtil.toString(optimizedPlan);
            String reference = (String)in.readObject();

			softAssert.assertEquals(reference, logicalPlan, "In test " + entry.getKey());
		}

		in.close();
		file.close();

		softAssert.assertAll();
	}
}
