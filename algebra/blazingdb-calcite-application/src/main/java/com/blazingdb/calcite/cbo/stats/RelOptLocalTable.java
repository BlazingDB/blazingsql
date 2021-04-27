package com.blazingdb.calcite.cbo.stats;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.hadoop.hive.ql.metadata.Table;

import java.util.List;

public class RelOptLocalTable implements RelOptTable {

    private double rowCount = -1;

    public RelOptLocalTable(double rowCount) {
        this.rowCount = rowCount;
    }

    @Override
    public List<String> getQualifiedName() {
        return null;
    }

    @Override
    public double getRowCount() { // <<<<<<<-----------We need local access to set this value as needed
        return rowCount;
    }

    @Override
    public RelDataType getRowType() {
        return null;
    }

    @Override
    public RelOptSchema getRelOptSchema() {
        return null;
    }

    @Override
    public RelNode toRel(ToRelContext context) {
        return null;
    }

    @Override
    public List<RelCollation> getCollationList() {
        return null;
    }

    @Override
    public RelDistribution getDistribution() {
        return null;
    }

    @Override
    public boolean isKey(ImmutableBitSet columns) {
        return false;
    }

    @Override
    public List<ImmutableBitSet> getKeys() {
        return null;
    }

    @Override
    public List<RelReferentialConstraint> getReferentialConstraints() {
        return null;
    }

    @Override
    public Expression getExpression(Class clazz) {
        return null;
    }

    @Override
    public RelOptTable extend(List<RelDataTypeField> extendedFields) {
        return null;
    }

    @Override
    public List<ColumnStrategy> getColumnStrategies() {
        return RelOptTableImpl.columnStrategies(this);
    }

    @Override
    public <C> C unwrap(Class<C> aClass) {
        return null;
    }
}
