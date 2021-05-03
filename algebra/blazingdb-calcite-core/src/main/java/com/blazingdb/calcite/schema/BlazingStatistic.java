package com.blazingdb.calcite.schema;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

public class BlazingStatistic implements Statistic {
    private final Double rowCount;

    public BlazingStatistic(Double rowCount) {
        this.rowCount = rowCount;
    }

    @Override
    public Double getRowCount() {
        return (double) rowCount;
    }

    @Override
    public boolean isKey(ImmutableBitSet columns) {
        return false;
    }

    @Override
    public List<ImmutableBitSet> getKeys() {
        return ImmutableList.of();
    }

    @Override
    public List<RelReferentialConstraint> getReferentialConstraints() {
        return ImmutableList.of();
    }

    @Override
    public List<RelCollation> getCollations() {
        return ImmutableList.of();
    }

    @Override
    public RelDistribution getDistribution() {
        return RelDistributionTraitDef.INSTANCE.getDefault();
    }
}
