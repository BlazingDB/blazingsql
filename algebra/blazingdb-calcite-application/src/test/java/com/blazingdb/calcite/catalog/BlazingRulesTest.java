package com.blazingdb.calcite.catalog;

import com.blazingdb.calcite.application.CalciteApplication;
import com.blazingdb.calcite.application.RelationalAlgebraGenerator;
import com.blazingdb.calcite.catalog.domain.CatalogColumnDataType;
import com.blazingdb.calcite.catalog.domain.CatalogColumnImpl;
import com.blazingdb.calcite.catalog.domain.CatalogDatabaseImpl;
import com.blazingdb.calcite.catalog.domain.CatalogTableImpl;
import com.blazingdb.calcite.catalog.repository.DatabaseRepository;
import com.blazingdb.calcite.optimizer.converter.*;
import com.blazingdb.calcite.optimizer.cost.DefaultRelMetadataProvider;
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

import org.apache.calcite.sql.SqlExplainLevel;
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

	@Test()
	public void
	generateSQLTestCBO() throws Exception {
		createTableSchemas();

		db = repo.getDatabase(dbId);

		BlazingSchema schema = new BlazingSchema(db);

		checkTable(schema, "customer");
		checkTable(schema, "orders");

		RelationalAlgebraGenerator algebraGen = new RelationalAlgebraGenerator(schema);

		System.out.println("<*****************************************************************************>");

		String sql =
//				"select c_custkey from `customer` inner join `orders` on c_custkey = o_custkey where c_custkey < 1000";
//				"select * from `customer`";
				"select c_custkey, c_name from customer where c_name='hello'";
//				"select c.c_custkey as customer_id, c.c_name as customer_name, n.n_name as nation_name, c.c_phone as customer_phone from customer c"
//				+ " join nation n on c.c_nationkey=n.n_nationkey ";

		RelNode nonOptimizedPlan = algebraGen.getNonOptimizedRelationalAlgebra(sql);
		System.out.println("non optimized\n");
		System.out.println(RelOptUtil.toString(nonOptimizedPlan, SqlExplainLevel.ALL_ATTRIBUTES) + "\n");

		List<RelOptRule> listRelOptRuleOpt = new ArrayList<RelOptRule>();
		listRelOptRuleOpt.add(CSVTableScanConverter.INSTANCE);
		listRelOptRuleOpt.add(CSVFilterConverter.INSTANCE);
		listRelOptRuleOpt.add(CSVProjectConverter.INSTANCE);
		listRelOptRuleOpt.add(CSVNewProjectConverter.INSTANCE);
		algebraGen.setRules(listRelOptRuleOpt);

		RelNode optimizedPlan = algebraGen.getOptimizedRelationalAlgebra(nonOptimizedPlan);
		System.out.println("optimized\n");
		System.out.println(RelOptUtil.toString(optimizedPlan, SqlExplainLevel.ALL_ATTRIBUTES) + "\n");

		List<RelOptRule> listRelOptRuleOptCBO = new ArrayList<RelOptRule>();
		listRelOptRuleOptCBO.add(CSVNewProjectRule.INSTANCE);

		RelNode optimizedPlanCBO = algebraGen.getOptimizedRelationalAlgebraCOB(optimizedPlan, listRelOptRuleOptCBO);
		System.out.println("cbo optimized\n");
		System.out.println(RelOptUtil.toString(optimizedPlanCBO, SqlExplainLevel.ALL_ATTRIBUTES) + "\n");
	}

	//TPCH queries
	List<Entry<String, String>> tpch_queries = Arrays.asList(
		new AbstractMap.SimpleEntry<String, String>("tpch01", "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate <= date '1998-12-01' - interval '90' day group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus"),
		new AbstractMap.SimpleEntry<String, String>("tpch02", "select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment from part, supplier, partsupp, nation, region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 15 and p_type like '%BRASS' and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'EUROPE' and ps_supplycost = ( select min(ps_supplycost) from partsupp, supplier, nation, region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'EUROPE' ) order by s_acctbal desc, n_name, s_name, p_partkey limit 100"),
		new AbstractMap.SimpleEntry<String, String>("tpch03", "select l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority from customer, orders, lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < date '1995-03-15' and l_shipdate > date '1995-03-15' group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate limit 10"),
		new AbstractMap.SimpleEntry<String, String>("tpch04", "select o_orderpriority, count(*) as order_count from orders where o_orderdate >= date '1993-07-01' and o_orderdate < date '1993-07-01' + interval '3' month and exists ( select * from lineitem where l_orderkey = o_orderkey and l_commitdate < l_receiptdate ) group by o_orderpriority order by o_orderpriority"),
		new AbstractMap.SimpleEntry<String, String>("tpch05", "select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue from customer, orders, lineitem, supplier, nation, region where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'ASIA' and o_orderdate >= date '1994-01-01' and o_orderdate < date '1994-01-01' + interval '1' year group by n_name order by revenue desc"),
		new AbstractMap.SimpleEntry<String, String>("tpch06", "select sum(l_extendedprice*l_discount) as revenue from lineitem where l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year and l_discount between 0.06 - 0.01 and 0.06 + 0.01 and l_quantity < 24"),
		new AbstractMap.SimpleEntry<String, String>("tpch07", "select supp_nation, cust_nation, l_year, sum(volume) as revenue from ( select n1.n_name as supp_nation, n2.n_name as cust_nation, extract(year from l_shipdate) as l_year, l_extendedprice * (1 - l_discount) as volume from supplier, lineitem, orders, customer, nation n1, nation n2 where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey and s_nationkey = n1.n_nationkey and c_nationkey = n2.n_nationkey and ( (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE') ) and l_shipdate between date '1995-01-01' and date '1996-12-31' ) as shipping group by supp_nation, cust_nation, l_year order by supp_nation, cust_nation, l_year"),
		new AbstractMap.SimpleEntry<String, String>("tpch08", "select o_year, sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share from ( select extract(year from o_orderdate) as o_year, l_extendedprice * (1-l_discount) as volume, n2.n_name as nation from part, supplier, lineitem, orders, customer, nation n1, nation n2, region where p_partkey = l_partkey and s_suppkey = l_suppkey and l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n1.n_nationkey and n1.n_regionkey = r_regionkey and r_name = 'AMERICA' and s_nationkey = n2.n_nationkey and o_orderdate between date '1995-01-01' and date '1996-12-31' and p_type = 'ECONOMY ANODIZED STEEL' ) as all_nations group by o_year order by o_year"),
		new AbstractMap.SimpleEntry<String, String>("tpch09", "select nation, o_year, sum(amount) as sum_profit from ( select n_name as nation, extract(year from o_orderdate) as o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount from part, supplier, lineitem, partsupp, orders, nation where s_suppkey = l_suppkey and ps_suppkey = l_suppkey and ps_partkey = l_partkey and p_partkey = l_partkey and o_orderkey = l_orderkey and s_nationkey = n_nationkey and p_name like '%green%' ) as profit group by nation, o_year order by nation, o_year desc"),
		new AbstractMap.SimpleEntry<String, String>("tpch10", "select c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as revenue, c_acctbal, n_name, c_address, c_phone, c_comment from customer, orders, lineitem, nation where c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate >= date '1993-10-01' and o_orderdate < date '1993-10-01' + interval '3' month and l_returnflag = 'R' and c_nationkey = n_nationkey group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment order by revenue desc limit 20"),
		new AbstractMap.SimpleEntry<String, String>("tpch11", "select ps_partkey, sum(ps_supplycost * ps_availqty) as valuep from partsupp, supplier, nation where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY' group by ps_partkey having sum(ps_supplycost * ps_availqty) > ( select sum(ps_supplycost * ps_availqty) * 0.0001 from partsupp, supplier, nation where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY' ) order by valuep desc"),
		new AbstractMap.SimpleEntry<String, String>("tpch12", "select l_shipmode, sum(case when o_orderpriority ='1-URGENT' or o_orderpriority ='2-HIGH' then 1 else 0 end) as high_line_count, sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count from orders, lineitem where o_orderkey = l_orderkey and l_shipmode in ('MAIL', 'SHIP') and l_commitdate < l_receiptdate and l_shipdate < l_commitdate and l_receiptdate >= date '1994-01-01' and l_receiptdate < date '1994-01-01' + interval '1' year group by l_shipmode order by l_shipmode"),
		new AbstractMap.SimpleEntry<String, String>("tpch13", "select c_count, count(*) as custdist from ( select c_custkey, count(o_orderkey) from customer left outer join orders on c_custkey = o_custkey and o_comment not like '%special%requests%' group by c_custkey )as c_orders (c_custkey, c_count) group by c_count order by custdist desc, c_count desc"),
		new AbstractMap.SimpleEntry<String, String>("tpch14", "select 100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice*(1-l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue from lineitem, part where l_partkey = p_partkey and l_shipdate >= date '1995-09-01' and l_shipdate < date '1995-09-01' + interval '1' month"),
		new AbstractMap.SimpleEntry<String, String>("tpch15", "with revenue (suplier_no, total_revenue) as ( select l_suppkey, sum(l_extendedprice * (1-l_discount)) from lineitem where l_shipdate >= date '1996-01-01' and l_shipdate < date '1996-01-01' + interval '3' month group by l_suppkey ) select s_suppkey, s_name, s_address, s_phone, total_revenue from supplier, revenue where s_suppkey = suplier_no and total_revenue = ( select max(total_revenue) from revenue ) order by s_suppkey"),
		new AbstractMap.SimpleEntry<String, String>("tpch16", "select p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt from partsupp, part where p_partkey = ps_partkey and p_brand <> 'Brand#45' and p_type not like 'MEDIUM POLISHED%' and p_size in (49, 14, 23, 45, 19, 3, 36, 9) and ps_suppkey not in ( select s_suppkey from supplier where s_comment like '%Customer%Complaints%' ) group by p_brand, p_type, p_size order by supplier_cnt desc, p_brand, p_type, p_size"),
		new AbstractMap.SimpleEntry<String, String>("tpch17", "select sum(l_extendedprice) / 7.0 as avg_yearly from lineitem, part where p_partkey = l_partkey and p_brand = 'Brand#23' and p_container = 'MED BOX' and l_quantity < ( select 0.2 * avg(l_quantity) from lineitem where l_partkey = p_partkey)"),
		new AbstractMap.SimpleEntry<String, String>("tpch18", "select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) from customer, orders, lineitem where o_orderkey in ( select l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > 300 ) and c_custkey = o_custkey and o_orderkey = l_orderkey group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice order by o_totalprice desc, o_orderdate limit 100"),
		new AbstractMap.SimpleEntry<String, String>("tpch19", "select sum(l_extendedprice * (1 - l_discount) ) as revenue from lineitem, part where ( p_partkey = l_partkey and p_brand = 'Brand#12' and p_container in ( 'SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') and l_quantity >= 1 and l_quantity <= 1 + 10 and p_size between 1 and 5 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ) or ( p_partkey = l_partkey and p_brand = 'Brand#23' and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') and l_quantity >= 10 and l_quantity <= 10 + 10 and p_size between 1 and 10 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ) or ( p_partkey = l_partkey and p_brand = 'Brand#34' and p_container in ( 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') and l_quantity >= 20 and l_quantity <= 20 + 10 and p_size between 1 and 15 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' )"),
		new AbstractMap.SimpleEntry<String, String>("tpch20", "select s_name, s_address from supplier, nation where s_suppkey in ( select ps_suppkey from partsupp where ps_partkey in ( select p_partkey from part where p_name like 'forest%' ) and ps_availqty > ( select 0.5 * sum(l_quantity) from lineitem where l_partkey = ps_partkey and l_suppkey = ps_suppkey and l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year ) ) and s_nationkey = n_nationkey and n_name = 'CANADA' order by s_name"),
		new AbstractMap.SimpleEntry<String, String>("tpch21", "select s_name, count(*) as numwait from supplier, lineitem l1, orders, nation where s_suppkey = l1.l_suppkey and o_orderkey = l1.l_orderkey and o_orderstatus = 'F' and l1.l_receiptdate > l1.l_commitdate and exists ( select * from lineitem l2 where l2.l_orderkey = l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey ) and not exists ( select * from lineitem l3 where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey and l3.l_receiptdate > l3.l_commitdate ) and s_nationkey = n_nationkey and n_name = 'SAUDI ARABIA' group by s_name order by numwait desc, s_name limit 100"),
		new AbstractMap.SimpleEntry<String, String>("tpch22", "select cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal from ( select substring(c_phone from 1 for 2) as cntrycode, c_acctbal from customer where substring(c_phone from 1 for 2) in ('13','31','23','29','30','18','17') and c_acctbal > ( select avg(c_acctbal) from customer where c_acctbal > 0.00 and substring (c_phone from 1 for 2) in ('13','31','23','29','30','18','17') ) and not exists ( select * from orders where o_custkey = c_custkey ) ) as custsale group by cntrycode order by cntrycode")
	);

	String reference_filename = "tpch-reference.bin";

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

			softAssert.assertEquals(logicalPlan, reference, "In test " + entry.getKey());
		}

		in.close();
		file.close();

		softAssert.assertAll();
	}
}
