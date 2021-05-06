package com.blazingdb.calcite.application;

import com.blazingdb.calcite.rules.FilterTableScanRule;
import com.blazingdb.calcite.rules.ProjectFilterTransposeRule;
import com.blazingdb.calcite.rules.ProjectTableScanRule;
import com.blazingdb.calcite.interpreter.BindableTableScan;
import com.blazingdb.calcite.schema.BlazingSchema;
import com.blazingdb.calcite.schema.BlazingTable;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.Lex;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.interpreter.InterpretableRel;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.*;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.tools.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;

import java.util.*;

/**
 * <h1>Generate Relational Algebra</h1>
 * The purpose of this class is to hold the planner, the program, and the
 * configuration for reuse based on a schema that is provided. It can then take
 * sql and convert it to relational algebra.
 *
 *
 * @author  Felipe Aramburu
 * @version 1.0
 * @since   2018-10-31
 */
public class RelationalAlgebraGenerator {
	final static Logger LOGGER = LoggerFactory.getLogger(RelationalAlgebraGenerator.class);

	/**
	 * Planner that converts sql to relational algebra.
	 */
	private Planner planner;
	/**
	 * Program which takes relational algebra and optimizes it
	 */
	private HepProgram program;
	/**
	 * Stores the context for the program hep planner. E.G. it stores the schema.
	 */
	private FrameworkConfig config;

	private List<RelOptRule> rules;

	private List<RelOptRule> rulesCBO;

	/**
	 * Constructor for the relational algebra generator class. It will take the
	 * schema store it in the  {@link #config} and then set up the  {@link
	 * #program} for optimizing and the  {@link #planner} for parsing.
	 *
	 * @param newSchema This is the schema which we will be using to validate our
	 *     query against.
	 * 					This gets stored in the {@link #config}
	 */
	public RelationalAlgebraGenerator(BlazingSchema newSchema) {
		try {
			Class.forName("org.apache.calcite.jdbc.Driver");

			Properties info = new Properties();
			info.setProperty("lex", "JAVA");
			Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
			CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
			calciteConnection.setSchema(newSchema.getName());

			SchemaPlus schema = calciteConnection.getRootSchema();

			schema.add(newSchema.getName(), newSchema);

			// schema.add("EMP", table);
			List<String> defaultSchema = new ArrayList<String>();
			defaultSchema.add(newSchema.getName());

			Properties props = new Properties();
			props.setProperty("defaultSchema", newSchema.getName());
			List<SqlOperatorTable> sqlOperatorTables = new ArrayList<>();
			sqlOperatorTables.add(SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
				EnumSet.of(SqlLibrary.STANDARD, SqlLibrary.ORACLE, SqlLibrary.MYSQL)));
			sqlOperatorTables.add(new CalciteCatalogReader(CalciteSchema.from(schema.getSubSchema(newSchema.getName())),
				defaultSchema,
				new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT),
				new CalciteConnectionConfigImpl(props)));

			config = Frameworks.newConfigBuilder()
						 .defaultSchema(schema.getSubSchema(newSchema.getName()))
						 .parserConfig(SqlParser.configBuilder().setLex(Lex.MYSQL).build())
						 .operatorTable(new ChainedSqlOperatorTable(sqlOperatorTables))
						 .build();

			planner = Frameworks.getPlanner(config);
		} catch(Exception e) {
			e.printStackTrace();

			config = null;
			planner = null;
			program = null;
		}
	}

	/** Direct constructor for testing purposes */
	public RelationalAlgebraGenerator(FrameworkConfig frameworkConfig, HepProgram hepProgram) {
		this.config = frameworkConfig;
		this.planner = Frameworks.getPlanner(frameworkConfig);
		this.program = hepProgram;
	}

	public void
	setRules(List<RelOptRule> rules) {
		this.rules = rules;
	}

	public void
	setRulesCBO(List<RelOptRule> rulesCBO) {
		this.rulesCBO = rulesCBO;
	}

	public SqlNode
	validateQuery(String sql) throws SqlSyntaxException, SqlValidationException {
		SqlNode tempNode;
		try {
			tempNode = planner.parse(sql);
		} catch(SqlParseException e) {
			planner.close();
			throw new SqlSyntaxException(sql, e);
		}

		SqlNode validatedSqlNode;
		try {
			validatedSqlNode = planner.validate(tempNode);
		} catch(ValidationException e) {
			planner.close();
			throw new SqlValidationException(sql, e);
		}
		return validatedSqlNode;
	}

	public RelNode
	getNonOptimizedRelationalAlgebra(String sql)
		throws SqlSyntaxException, SqlValidationException, RelConversionException {
		SqlNode validatedSqlNode = validateQuery(sql);
		return planner.rel(validatedSqlNode).project();
	}

	public RelNode
	getOptimizedRelationalAlgebra(RelNode nonOptimizedPlan) throws RelConversionException {
		if(rules == null) {
			// TODO: this is a temporal workaround due to the issue with ProjectToWindowRule
			// with constant values like  LEAD($1, 5)
			if (RelOptUtil.toString(nonOptimizedPlan).indexOf("OVER") != -1) {
				program = new HepProgramBuilder()
					      //.addRuleInstance(ProjectToWindowRule.PROJECT)
						  .addRuleInstance(AggregateExpandDistinctAggregatesRule.JOIN)
						  .addRuleInstance(FilterAggregateTransposeRule.INSTANCE)
						  .addRuleInstance(FilterJoinRule.JoinConditionPushRule.FILTER_ON_JOIN)
						  .addRuleInstance(FilterJoinRule.JoinConditionPushRule.JOIN)
						  .addRuleInstance(ProjectMergeRule.INSTANCE)
						  .addRuleInstance(FilterMergeRule.INSTANCE)
						  //.addRuleInstance(ProjectJoinTransposeRule.INSTANCE)
						  .addRuleInstance(ProjectFilterTransposeRule.INSTANCE)

						  //The following three rules evaluate expressions in Projects and Filters
						  //.addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE)
						  .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)

						  .addRuleInstance(ProjectTableScanRule.INSTANCE)
						  .addRuleInstance(FilterTableScanRule.INSTANCE)
						  .addRuleInstance(FilterRemoveIsNotDistinctFromRule.INSTANCE)
						  .addRuleInstance(AggregateReduceFunctionsRule.INSTANCE)
						  .build();
			} else {
				program = new HepProgramBuilder()
						  //.addRuleInstance(ProjectToWindowRule.PROJECT)
						  .addRuleInstance(AggregateExpandDistinctAggregatesRule.JOIN)
						  .addRuleInstance(FilterAggregateTransposeRule.INSTANCE)
						  .addRuleInstance(FilterJoinRule.JoinConditionPushRule.FILTER_ON_JOIN)
						  .addRuleInstance(FilterJoinRule.JoinConditionPushRule.JOIN)
						  .addRuleInstance(ProjectMergeRule.INSTANCE)
						  .addRuleInstance(FilterMergeRule.INSTANCE)
						  .addRuleInstance(ProjectJoinTransposeRule.INSTANCE)
						  .addRuleInstance(ProjectTableScanRule.INSTANCE)
						  .addRuleInstance(ProjectFilterTransposeRule.INSTANCE)

						  //The following three rules evaluate expressions in Projects and Filters
						  .addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE)
						  .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)

						  .addRuleInstance(ProjectTableScanRule.INSTANCE)
						  .addRuleInstance(FilterTableScanRule.INSTANCE)
						  .addRuleInstance(FilterRemoveIsNotDistinctFromRule.INSTANCE)
						  .addRuleInstance(AggregateReduceFunctionsRule.INSTANCE)
						  .build();
			}
		} else {
			HepProgramBuilder programBuilder = new HepProgramBuilder();
			for(RelOptRule rule : rules) {
				programBuilder = programBuilder.addRuleInstance(rule);
			}
			program = programBuilder.build();
		}

		final HepPlanner hepPlanner = new HepPlanner(program, config.getContext());
		nonOptimizedPlan.getCluster().getPlanner().setExecutor(new RexExecutorImpl(null));
		hepPlanner.setRoot(nonOptimizedPlan);

		planner.close();

		return hepPlanner.findBestExp();
	}

	public RelNode
	getOptimizedRelationalAlgebraCBO(RelNode rboOptimizedPlan) throws RelConversionException {
		VolcanoPlanner planner = (VolcanoPlanner) rboOptimizedPlan.getCluster().getPlanner();
		planner.clear();

		if(rules == null){
			if (RelOptUtil.toString(rboOptimizedPlan).indexOf("OVER") != -1) {
				//RBO Rules
				planner.addRule(AggregateExpandDistinctAggregatesRule.JOIN);
				planner.addRule(FilterAggregateTransposeRule.INSTANCE);
				planner.addRule(FilterJoinRule.JoinConditionPushRule.FILTER_ON_JOIN);
				planner.addRule(FilterJoinRule.JoinConditionPushRule.JOIN);
				planner.addRule(ProjectMergeRule.INSTANCE);
				planner.addRule(FilterMergeRule.INSTANCE);
				//RBO BSQL Custom Rules
				planner.addRule(com.blazingdb.calcite.rules.ProjectFilterTransposeRule.INSTANCE);
				planner.addRule(com.blazingdb.calcite.rules.ReduceExpressionsRule.FILTER_INSTANCE);
				planner.addRule(com.blazingdb.calcite.rules.ProjectTableScanRule.INSTANCE);
				planner.addRule(com.blazingdb.calcite.rules.FilterTableScanRule.INSTANCE);
				//RBO Rules
				planner.addRule(FilterRemoveIsNotDistinctFromRule.INSTANCE);
				planner.addRule(AggregateReduceFunctionsRule.INSTANCE);
			} else {
				//RBO Rules
				planner.addRule(AggregateExpandDistinctAggregatesRule.JOIN);
				planner.addRule(FilterAggregateTransposeRule.INSTANCE);
				planner.addRule(FilterJoinRule.JoinConditionPushRule.FILTER_ON_JOIN);
				planner.addRule(FilterJoinRule.JoinConditionPushRule.JOIN);
				planner.addRule(ProjectMergeRule.INSTANCE);
				planner.addRule(FilterMergeRule.INSTANCE);
				//RBO BSQL Custom Rules
				planner.addRule(com.blazingdb.calcite.rules.ProjectJoinTransposeRule.INSTANCE);
				planner.addRule(com.blazingdb.calcite.rules.ProjectTableScanRule.INSTANCE);
				planner.addRule(com.blazingdb.calcite.rules.ProjectFilterTransposeRule.INSTANCE);
				planner.addRule(com.blazingdb.calcite.rules.ReduceExpressionsRule.PROJECT_INSTANCE);
				planner.addRule(com.blazingdb.calcite.rules.ReduceExpressionsRule.FILTER_INSTANCE);
				planner.addRule(com.blazingdb.calcite.rules.ProjectTableScanRule.INSTANCE);
				planner.addRule(com.blazingdb.calcite.rules.FilterTableScanRule.INSTANCE);
				//RBO Rules
				planner.addRule(FilterRemoveIsNotDistinctFromRule.INSTANCE);
				planner.addRule(AggregateReduceFunctionsRule.INSTANCE);
			}
		} else {
			for(RelOptRule ruleRBO : rules) {
				planner.addRule(ruleRBO);
			}
		}

		if(rulesCBO == null){
			planner.addRule(Bindables.BINDABLE_AGGREGATE_RULE);
			planner.addRule(Bindables.BINDABLE_FILTER_RULE);
			planner.addRule(Bindables.BINDABLE_JOIN_RULE);
			planner.addRule(Bindables.BINDABLE_TABLE_SCAN_RULE);
			planner.addRule(Bindables.BINDABLE_PROJECT_RULE);
			planner.addRule(Bindables.BINDABLE_SORT_RULE);
			planner.addRule(JoinAssociateRule.INSTANCE); //to support: a x b x c = b x c x a
//			planner.addRule(JoinPushThroughJoinRule.LEFT);
//			planner.addRule(JoinPushThroughJoinRule.RIGHT);
		} else {
			for(RelOptRule ruleCBO : rulesCBO) {
				planner.addRule(ruleCBO);
			}
		}

		rboOptimizedPlan = planner.changeTraits(rboOptimizedPlan, rboOptimizedPlan.getCluster().traitSet().replace(BindableConvention.INSTANCE));
//		rboOptimizedPlan = planner.changeTraits(rboOptimizedPlan, rboOptimizedPlan.getCluster().traitSet().replace(EnumerableConvention.INSTANCE));
		rboOptimizedPlan.getCluster().getPlanner().setRoot(rboOptimizedPlan);

		return planner.chooseDelegate().findBestExp();
	}

	/**
	 * Takes a sql statement and converts it into an optimized relational algebra
	 * node. The result of this function is a logical plan that has been optimized
	 * using a rule based optimizer.
	 *
	 * @param sql a string sql query that is to be parsed, converted into
	 *     relational algebra, then optimized
	 * @return a RelNode which contains the relational algebra tree generated from
	 *     the sql statement provided after
	 * 			an optimization step has been completed.
	 * @throws SqlSyntaxException, SqlValidationException, RelConversionException
	 */
	public RelNode
	getRelationalAlgebra(String sql) throws SqlSyntaxException, SqlValidationException, RelConversionException {
		RelNode nonOptimizedPlan = getNonOptimizedRelationalAlgebra(sql);
		LOGGER.debug("non optimized\n" + RelOptUtil.toString(nonOptimizedPlan, SqlExplainLevel.ALL_ATTRIBUTES));

		RelNode optimizedPlan = getOptimizedRelationalAlgebra(nonOptimizedPlan);
		LOGGER.debug("rbo optimized\n" + RelOptUtil.toString(optimizedPlan, SqlExplainLevel.ALL_ATTRIBUTES));

		return optimizedPlan;
	}

	/**
	 * Takes a sql statement and converts it into an optimized relational algebra
	 * node using CBO Cost Based Optimizer thru {@link org.apache.calcite.plan.volcano.VolcanoPlanner}.
	 * The result of this function is a physical plan that has been optimized using a cost based optimizer.
	 *
	 * @param sql a string sql query that is to be parsed, converted into
	 *     relational algebra, then optimized
	 * @return a RelNode which contains the relational algebra tree generated from
	 *     the sql statement provided after
	 * 			an optimization step has been completed.
	 * @throws SqlSyntaxException, SqlValidationException, RelConversionException
	 */
	public RelNode
	getRelationalAlgebraCBO(String sql) throws SqlSyntaxException, SqlValidationException, RelConversionException {
		RelNode nonOptimizedPlan = getNonOptimizedRelationalAlgebra(sql);
		LOGGER.debug("non optimized\n" + RelOptUtil.toString(nonOptimizedPlan, SqlExplainLevel.ALL_ATTRIBUTES));

		RelNode optimizedPlan = getOptimizedRelationalAlgebraCBO(nonOptimizedPlan);
		LOGGER.debug("cbo optimized\n" + RelOptUtil.toString(optimizedPlan, SqlExplainLevel.ALL_ATTRIBUTES));

		return optimizedPlan;
	}

	public String
	getRelationalAlgebraString(String sql) throws SqlSyntaxException, SqlValidationException, RelConversionException {
		String response = "";

		try {
			response = RelOptUtil.toString(getRelationalAlgebra(sql));
		}catch(SqlValidationException ex){
			//System.out.println(ex.getMessage());
			//System.out.println("Found validation err!");
			throw ex;
			//return "fail: \n " + ex.getMessage();
		}catch(SqlSyntaxException ex){
			//System.out.println(ex.getMessage());
			//System.out.println("Found syntax err!");
			throw ex;
			//return "fail: \n " + ex.getMessage();
		} catch(Exception ex) {
			//System.out.println(ex.toString());
			//System.out.println(ex.getMessage());
			ex.printStackTrace();

			LOGGER.error(ex.getMessage());
			return "rbo fail: \n " + ex.getMessage();
		}

		return response;
	}

	public String
	getRelationalAlgebraCBOString(String sql) throws SqlSyntaxException, SqlValidationException, RelConversionException {
		String response = "";

		try {
			response = RelOptUtil.toString(getRelationalAlgebraCBO(sql));
		}catch(SqlValidationException ex){
			//System.out.println(ex.getMessage());
			//System.out.println("Found validation err!");
			throw ex;
			//return "fail: \n " + ex.getMessage();
		}catch(SqlSyntaxException ex){
			//System.out.println(ex.getMessage());
			//System.out.println("Found syntax err!");
			throw ex;
			//return "fail: \n " + ex.getMessage();
		} catch(Exception ex) {
			//System.out.println(ex.toString());
			//System.out.println(ex.getMessage());
			ex.printStackTrace();

			LOGGER.error(ex.getMessage());
			return "cbo fail: \n " + ex.getMessage();
		}

		return response;
	}
}
