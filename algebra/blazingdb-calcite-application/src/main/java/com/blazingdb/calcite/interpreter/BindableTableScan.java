/*
 * This file is a copy with some modifications of the BindableTableScan from
 * the Apache Calcite project. The original code can be found at:
 * https://github.com/apache/calcite/blob/branch-1.23/core/src/main/java/org/apache/calcite/interpreter/Bindables.java
 * The changes are about expecting a new parameter that will hold the column aliases.
 */
package com.blazingdb.calcite.interpreter;

import org.apache.calcite.DataContext;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.interpreter.BindableRel;
import org.apache.calcite.interpreter.Node;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.ImmutableIntList;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/**
 * Scan of a table that implements {@link ScannableTable} and therefore can
 * be converted into an {@link Enumerable}.
 */
public class BindableTableScan extends TableScan implements BindableRel {
	public final ImmutableList<RexNode> filters;
	public final ImmutableIntList projects;
	public final ImmutableList<String> aliases;

	/** Creates a BindableTableScan.
	 *
	 * <p>Use {@link #create} unless you know what you are doing. */
	BindableTableScan(RelOptCluster cluster,
		RelTraitSet traitSet,
		RelOptTable table,
		ImmutableList<RexNode> filters,
		ImmutableIntList projects,
		ImmutableList<String> aliases) {
		super(cluster, traitSet, ImmutableList.of(), table);
		this.filters = Objects.requireNonNull(filters);
		this.projects = Objects.requireNonNull(projects);
		this.aliases = Objects.requireNonNull(aliases);
		Preconditions.checkArgument(canHandle(table));
	}

	/** Creates a BindableTableScan. */
	public static BindableTableScan
	create(RelOptCluster cluster, RelOptTable relOptTable) {
		return create(cluster, relOptTable, ImmutableList.of(), identity(relOptTable), ImmutableList.<String>of());
	}

	/** Creates a BindableTableScan. */
	public static BindableTableScan
	create(RelOptCluster cluster,
		RelOptTable relOptTable,
		List<RexNode> filters,
		List<Integer> projects,
		List<String> aliases) {
		final Table table = relOptTable.unwrap(Table.class);
		final RelTraitSet traitSet =
			cluster.traitSetOf(BindableConvention.INSTANCE).replaceIfs(RelCollationTraitDef.INSTANCE, () -> {
				if(table != null) {
					return table.getStatistic().getCollations();
				}
				return ImmutableList.of();
			});
		return new BindableTableScan(cluster,
			traitSet,
			relOptTable,
			ImmutableList.copyOf(filters),
			ImmutableIntList.copyOf(projects),
			ImmutableList.copyOf(aliases));
	}

	@Override
	public RelDataType
	deriveRowType() {
		final RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();
		final List<RelDataTypeField> fieldList = table.getRowType().getFieldList();
		for(int project : projects) {
			builder.add(fieldList.get(project));
		}
		return builder.build();
	}

	public Class<Object[]>
	getElementType() {
		return Object[].class;
	}

	@Override
	public RelWriter
	explainTerms(RelWriter pw) {
		return super.explainTerms(pw)
			.itemIf("filters", filters, !filters.isEmpty())
			.itemIf("projects", projects, !projects.equals(identity()))
			.itemIf("aliases", aliases, !aliases.isEmpty());
	}

	@Override
	public RelOptCost
	computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
		boolean noPushing = filters.isEmpty() && projects.size() == table.getRowType().getFieldCount();
		if(noPushing) {
			return super.computeSelfCost(planner, mq);
		}
		// Cost factor for pushing filters
		double f = filters.isEmpty() ? 1d : 0.5d;

		// Cost factor for pushing fields
		// The "+ 2d" on top and bottom keeps the function fairly smooth.
		double p = ((double) projects.size() + 2d) / ((double) table.getRowType().getFieldCount() + 2d);

		// Multiply the cost by a factor that makes a scan more attractive if
		// filters and projects are pushed to the table scan
		return super.computeSelfCost(planner, mq).multiplyBy(f * p * 0.01d);
	}

	public static boolean
	canHandle(RelOptTable table) {
		return table.unwrap(ScannableTable.class) != null || table.unwrap(FilterableTable.class) != null ||
			table.unwrap(ProjectableFilterableTable.class) != null;
	}

	public Enumerable<Object[]>
	bind(DataContext dataContext) {
		if(table.unwrap(ProjectableFilterableTable.class) != null) {
			return table.unwrap(ProjectableFilterableTable.class).scan(dataContext, filters, projects.toIntArray());
		} else if(table.unwrap(FilterableTable.class) != null) {
			return table.unwrap(FilterableTable.class).scan(dataContext, filters);
		} else {
			return table.unwrap(ScannableTable.class).scan(dataContext);
		}
	}

	public Node
	implement(InterpreterImplementor implementor) {
		throw new UnsupportedOperationException();  // TODO:
	}
}