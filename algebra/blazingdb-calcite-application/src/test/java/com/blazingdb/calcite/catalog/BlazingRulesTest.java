package com.blazingdb.calcite.catalog;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.AbstractMap.SimpleEntry;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

import com.blazingdb.calcite.application.RelationalAlgebraGenerator;
import com.blazingdb.calcite.catalog.domain.CatalogColumnDataType;
import com.blazingdb.calcite.catalog.domain.CatalogColumnImpl;
import com.blazingdb.calcite.catalog.domain.CatalogDatabaseImpl;
import com.blazingdb.calcite.catalog.domain.CatalogTableImpl;
import com.blazingdb.calcite.catalog.repository.DatabaseRepository;
import com.blazingdb.calcite.application.CalciteApplication;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterTableScanRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectTableScanRule;

import com.blazingdb.calcite.schema.BlazingSchema;

import org.apache.calcite.schema.Table;

public class BlazingRulesTest {

  private static SessionFactory sessionFactory = null;

  @BeforeMethod
  public void setUp() throws Exception {
    CalciteApplication.executeUpdate();
    repo = new DatabaseRepository();
    sessionFactory = new Configuration().configure().buildSessionFactory();
    db = new CatalogDatabaseImpl("main");
  }

  @AfterMethod
  public void tearDown() throws Exception {
    repo.dropDatabase(db);
    sessionFactory.close();
  }

  DatabaseRepository repo;
  CatalogDatabaseImpl db;
  private Long dbId = -1L;

  public void createTableSchemas() throws Exception {
    System.out.println(
        "=============================== GENERATE RELATIONAL ALGEBRA WITH DIFFERENT RULES TEST ====================================");
    final long startTime = System.currentTimeMillis();

    repo.createDatabase(db);
    dbId = db.getId();

    Map<String, List<Entry<String, CatalogColumnDataType>>> map = new HashMap<>();
      map.put( "customer",
        Arrays.asList(
            new SimpleEntry<>("c_custkey", CatalogColumnDataType.GDF_INT32),
            new SimpleEntry<>("c_name", CatalogColumnDataType.GDF_STRING),
            new SimpleEntry<>("c_address", CatalogColumnDataType.GDF_STRING),
            new SimpleEntry<>("c_nationkey", CatalogColumnDataType.GDF_INT32),
            new SimpleEntry<>("c_phone", CatalogColumnDataType.GDF_STRING),
            new SimpleEntry<>("c_acctbal", CatalogColumnDataType.GDF_FLOAT64),
            new SimpleEntry<>("c_mktsegment", CatalogColumnDataType.GDF_STRING),
            new SimpleEntry<>("c_comment", CatalogColumnDataType.GDF_STRING)));
      map.put( "region",
        Arrays.asList(
            new SimpleEntry<>("r_regionkey", CatalogColumnDataType.GDF_INT32),
            new SimpleEntry<>("r_name", CatalogColumnDataType.GDF_STRING),
            new SimpleEntry<>("r_comment", CatalogColumnDataType.GDF_STRING)));
      map.put( "nation",
        Arrays.asList(
            new SimpleEntry<>("n_nationkey", CatalogColumnDataType.GDF_INT32),
            new SimpleEntry<>("n_name", CatalogColumnDataType.GDF_STRING),
            new SimpleEntry<>("n_regionkey", CatalogColumnDataType.GDF_INT32),
            new SimpleEntry<>("n_comment", CatalogColumnDataType.GDF_STRING)));
      map.put( "orders",
        Arrays.asList(
            new SimpleEntry<>("o_orderkey", CatalogColumnDataType.GDF_INT64),
            new SimpleEntry<>("o_custkey", CatalogColumnDataType.GDF_INT32),
            new SimpleEntry<>("o_orderstatus",
                              CatalogColumnDataType.GDF_STRING),
            new SimpleEntry<>("o_totalprice",
                              CatalogColumnDataType.GDF_FLOAT64),
            new SimpleEntry<>("o_orderdate", CatalogColumnDataType.GDF_DATE64),
            new SimpleEntry<>("o_orderpriority",
                              CatalogColumnDataType.GDF_STRING),
            new SimpleEntry<>("o_clerk", CatalogColumnDataType.GDF_STRING),
            new SimpleEntry<>("o_shippriority",
                              CatalogColumnDataType.GDF_STRING),
            new SimpleEntry<>("o_comment", CatalogColumnDataType.GDF_STRING)));
      map.put( "supplier",
        Arrays.asList(
            new SimpleEntry<>("s_suppkey", CatalogColumnDataType.GDF_INT64),
            new SimpleEntry<>("s_name", CatalogColumnDataType.GDF_STRING),
            new SimpleEntry<>("s_address", CatalogColumnDataType.GDF_STRING),
            new SimpleEntry<>("s_nationkey", CatalogColumnDataType.GDF_INT32),
            new SimpleEntry<>("s_phone", CatalogColumnDataType.GDF_STRING),
            new SimpleEntry<>("s_acctbal", CatalogColumnDataType.GDF_FLOAT64),
            new SimpleEntry<>("s_comment", CatalogColumnDataType.GDF_STRING)));
      map.put( "lineitem",
        Arrays.asList(
            new SimpleEntry<>("l_orderkey", CatalogColumnDataType.GDF_INT64),
            new SimpleEntry<>("l_partkey", CatalogColumnDataType.GDF_INT64),
            new SimpleEntry<>("l_suppkey", CatalogColumnDataType.GDF_INT64),
            new SimpleEntry<>("l_linenumber", CatalogColumnDataType.GDF_INT32),
            new SimpleEntry<>("l_quantity", CatalogColumnDataType.GDF_FLOAT64),
            new SimpleEntry<>("l_extendedprice",
                              CatalogColumnDataType.GDF_FLOAT64),
            new SimpleEntry<>("l_discount", CatalogColumnDataType.GDF_FLOAT64),
            new SimpleEntry<>("l_tax", CatalogColumnDataType.GDF_FLOAT64),
            new SimpleEntry<>("l_returnflag", CatalogColumnDataType.GDF_STRING),
            new SimpleEntry<>("l_linestatus", CatalogColumnDataType.GDF_STRING),
            new SimpleEntry<>("l_shipdate", CatalogColumnDataType.GDF_DATE64),
            new SimpleEntry<>("l_commitdate", CatalogColumnDataType.GDF_DATE64),
            new SimpleEntry<>("l_receiptdate",
                              CatalogColumnDataType.GDF_DATE64),
            new SimpleEntry<>("l_shipinstruct",
                              CatalogColumnDataType.GDF_STRING),
            new SimpleEntry<>("l_shipmode", CatalogColumnDataType.GDF_STRING),
            new SimpleEntry<>("l_comment", CatalogColumnDataType.GDF_STRING)));
      map.put( "part",
        Arrays.asList(
            new SimpleEntry<>("p_partkey", CatalogColumnDataType.GDF_INT64),
            new SimpleEntry<>("p_name", CatalogColumnDataType.GDF_STRING),
            new SimpleEntry<>("p_mfgr", CatalogColumnDataType.GDF_STRING),
            new SimpleEntry<>("p_brand", CatalogColumnDataType.GDF_STRING),
            new SimpleEntry<>("p_type", CatalogColumnDataType.GDF_STRING),
            new SimpleEntry<>("p_size", CatalogColumnDataType.GDF_INT64),
            new SimpleEntry<>("p_container", CatalogColumnDataType.GDF_STRING),
            new SimpleEntry<>("p_retailprice",
                              CatalogColumnDataType.GDF_FLOAT32),
            new SimpleEntry<>("p_comment", CatalogColumnDataType.GDF_STRING)));
      map.put( "partsupp",
        Arrays.asList(
            new SimpleEntry<>("ps_partkey", CatalogColumnDataType.GDF_INT64),
            new SimpleEntry<>("ps_suppkey", CatalogColumnDataType.GDF_INT64),
            new SimpleEntry<>("ps_availqty", CatalogColumnDataType.GDF_INT64),
            new SimpleEntry<>("ps_supplycost",
                              CatalogColumnDataType.GDF_FLOAT32),
            new SimpleEntry<>("ps_comment", CatalogColumnDataType.GDF_STRING)));

    for (Map.Entry<String, List<Entry<String, CatalogColumnDataType>>> entry :
         map.entrySet()) {

      List<CatalogColumnImpl> columns = new ArrayList<CatalogColumnImpl>();

      int order_value = 0;
      for (Entry<String, CatalogColumnDataType> field : entry.getValue()) {
        columns.add(new CatalogColumnImpl(field.getKey(), field.getValue(),
                                          order_value++));
      }

      CatalogTableImpl table =
          new CatalogTableImpl(entry.getKey(), db, columns);

      db.addTable(table);
      repo.updateDatabase(db);
    }

    final long endTime = System.currentTimeMillis();
    System.out.println("Total execution time: " + (endTime - startTime));
  }

  public void checkTable(BlazingSchema schema, String table_name) throws Exception {
    Table tableTemp1 = schema.getTable(table_name);
    if (tableTemp1 == null) {
      System.out.println("table " + table_name + " NOT found");
      throw new Exception();
    } else {
      System.out.println("table found");
    }
  }

  @Test()
  public void generateSQLTest() throws Exception {

    createTableSchemas();

    db = repo.getDatabase(dbId);

    BlazingSchema schema = new BlazingSchema(db);

    checkTable(schema, "customer");
    checkTable(schema, "orders");

    RelationalAlgebraGenerator algebraGen =
        new RelationalAlgebraGenerator(schema);

    List<List<RelOptRule>> rulesSet = new ArrayList<List<RelOptRule>>();

    List<RelOptRule> rules1 = Arrays.asList(
        ProjectFilterTransposeRule.INSTANCE,
        FilterJoinRule.JoinConditionPushRule.FILTER_ON_JOIN,
        FilterJoinRule.JoinConditionPushRule.JOIN, ProjectMergeRule.INSTANCE,
        FilterMergeRule.INSTANCE, ProjectJoinTransposeRule.INSTANCE,
        ProjectTableScanRule.INSTANCE);

    rulesSet.add(rules1);

    List<RelOptRule> rules2 = Arrays.asList(
        FilterJoinRule.JoinConditionPushRule.FILTER_ON_JOIN,
        FilterJoinRule.JoinConditionPushRule.JOIN, ProjectMergeRule.INSTANCE,
        FilterMergeRule.INSTANCE, ProjectJoinTransposeRule.INSTANCE,
        ProjectFilterTransposeRule.INSTANCE, ProjectTableScanRule.INSTANCE);

    rulesSet.add(rules2);

    for (List<RelOptRule> rules : rulesSet) {
      System.out.println(
          "<*****************************************************************************>");

      String sql =
          "select c_custkey from `customer` inner join `orders` on c_custkey = o_custkey where c_custkey < 1000";
      RelNode nonOptimizedPlan =
          algebraGen.getNonOptimizedRelationalAlgebra(sql);
      System.out.println("non optimized\n");
      System.out.println(RelOptUtil.toString(nonOptimizedPlan) + "\n");

      for (int I = 0; I < rules.size(); I++) {
        algebraGen.setRules(rules.subList(0, I + 1));

        RelNode optimizedPlan =
            algebraGen.getOptimizedRelationalAlgebra(nonOptimizedPlan);

        System.out.println("optimized by rule: " + rules.get(I).getClass().getName() + "\n");
        System.out.println(RelOptUtil.toString(optimizedPlan) + "\n");
      }
    }
  }
}
