/*
 * This file is a copy with some modifications of the ProjectTableScanRule from
 * the Apache Calcite project. The original code can be found at:
 * https://github.com/apache/calcite/blob/branch-1.23/core/src/main/java/org/apache/calcite/rel/rules/ProjectTableScanRule.java
 * The changes are about passing the column aliases extracted from the Projection
 * to our customized BindableTableScan.
 */
package com.blazingdb.calcite.rules;

import com.blazingdb.calcite.interpreter.BindableTableScan;

import org.apache.calcite.adapter.enumerable.EnumerableInterpreter;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Planner rule that converts a {@link Project}
 * on a {@link org.apache.calcite.rel.core.TableScan}
 * of a {@link org.apache.calcite.schema.ProjectableFilterableTable}
 * to a {@link org.apache.calcite.interpreter.Bindables.BindableTableScan}.
 *
 * <p>The {@link #INTERPRETER} variant allows an intervening
 * {@link org.apache.calcite.adapter.enumerable.EnumerableInterpreter}.
 *
 * @see FilterTableScanRule
 */
public abstract class ProjectTableScanRule extends RelOptRule {
	@SuppressWarnings("Guava")
	@Deprecated  // to be removed before 2.0
	public static final com.google.common.base.Predicate<TableScan> PREDICATE = ProjectTableScanRule::test;

	/** Rule that matches Project on TableScan. */
	public static final ProjectTableScanRule INSTANCE = new ProjectTableScanRule(
		operand(Project.class, operandJ(TableScan.class, null, ProjectTableScanRule::test, none())),
		RelFactories.LOGICAL_BUILDER,
		"ProjectScanRule") {
		@Override
		public void onMatch(RelOptRuleCall call) {
			final Project project = call.rel(0);
			final TableScan scan = call.rel(1);
			apply(call, project, scan);
		}
	};

	/** Rule that matches Project on EnumerableInterpreter on TableScan. */
	public static final ProjectTableScanRule INTERPRETER = new ProjectTableScanRule(
		operand(Project.class,
			operand(EnumerableInterpreter.class, operandJ(TableScan.class, null, ProjectTableScanRule::test, none()))),
		RelFactories.LOGICAL_BUILDER,
		"ProjectScanRule:interpreter") {
		@Override
		public void onMatch(RelOptRuleCall call) {
			final Project project = call.rel(0);
			final TableScan scan = call.rel(2);
			apply(call, project, scan);
		}
	};

	//~ Constructors -----------------------------------------------------------

	/** Creates a ProjectTableScanRule. */
	public ProjectTableScanRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String description) {
		super(operand, relBuilderFactory, description);
	}

	//~ Methods ----------------------------------------------------------------

	protected static boolean
	test(TableScan scan) {
		// We can only push projects into a ProjectableFilterableTable.
		final RelOptTable table = scan.getTable();
		return table.unwrap(ProjectableFilterableTable.class) != null && !(scan instanceof BindableTableScan);
	}

	protected void
	apply(RelOptRuleCall call, Project project, TableScan scan) {
		final RelOptTable table = scan.getTable();
		assert table.unwrap(ProjectableFilterableTable.class) != null;

		final List<Integer> selectedColumns = new ArrayList<>();
		project.getProjects().forEach(proj -> {
			proj.accept(new RexVisitorImpl<Void>(true) {
				public Void visitInputRef(RexInputRef inputRef) {
					if(!selectedColumns.contains(inputRef.getIndex())) {
						selectedColumns.add(inputRef.getIndex());
					}
					return null;
				}
			});
		});

		final List<RexNode> filtersPushDown;
		final List<Integer> projectsPushDown;
		if(scan instanceof Bindables.BindableTableScan) {
			final Bindables.BindableTableScan bindableScan = (Bindables.BindableTableScan) scan;
			filtersPushDown = bindableScan.filters;
			projectsPushDown =
				selectedColumns.stream().map(col -> bindableScan.projects.get(col)).collect(Collectors.toList());
		} else {
			filtersPushDown = ImmutableList.of();
			projectsPushDown = selectedColumns;
		}
		final List<String> aliases = new ArrayList<>();
		List<String> col_names = table.getRowType().getFieldNames();
		int count_cols = 0;
		for(Pair<RexNode, String> value : project.getNamedProjects()) {
			// We want to mantain column names when no aliases where provided
			// If we have in the future issues with aliases we can review:
			// PR related to this -> https://github.com/BlazingDB/blazingsql/pull/1400
			// Issue related to this -> https://github.com/BlazingDB/blazingsql/issues/1393
			if (value.right.length() > 1 && value.right.substring(0,2).equals("$f") && count_cols < projectsPushDown.size() ) {
				aliases.add(col_names.get(projectsPushDown.get(count_cols)));
			}
			else {
				aliases.add(value.right);
			}
			count_cols++;
		}
		BindableTableScan newScan =
			BindableTableScan.create(scan.getCluster(), scan.getTable(), filtersPushDown, projectsPushDown, aliases);
		Mapping mapping = Mappings.target(selectedColumns, scan.getRowType().getFieldCount());
		final List<RexNode> newProjectRexNodes = ImmutableList.copyOf(RexUtil.apply(mapping, project.getProjects()));

		if(RexUtil.isIdentity(newProjectRexNodes, newScan.getRowType())) {
			call.transformTo(newScan);
		} else {
			call.transformTo(call.builder().push(newScan).project(newProjectRexNodes, aliases).build());
		}
	}
}