package com.blazingdb.calcite.cbo.reloperators;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.TraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveRelNode;

public class LocalFilter extends Filter implements LocalRelNode {
    public static class StatEnhancedLocalFilter extends LocalFilter {

        private long rowCount;

        // FIXME: use a generic proxy wrapper to create runtimestat enhanced nodes
        public StatEnhancedLocalFilter(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition,
                                      long rowCount) {
            super(cluster, traits, child, condition);
            this.rowCount = rowCount;
        }

        public long getRowCount() {
            return rowCount;
        }

        @Override
        public double estimateRowCount(RelMetadataQuery mq) {
            return rowCount;
        }

        @Override
        public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
            return super.computeSelfCost(planner, mq);
        }

        @Override
        public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
            assert traitSet.containsIfApplicable(HiveRelNode.CONVENTION);
            return new StatEnhancedLocalFilter(getCluster(), traitSet, input, condition, rowCount);
        }

    }

    public LocalFilter(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
        super(cluster, traits, child, condition);
    }

    public LocalFilter(RelInput input) {
        super(input);
    }

    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        assert traitSet.containsIfApplicable(LocalRelNode.CONVENTION);
        return new LocalFilter(getCluster(), traitSet, input, condition);
    }
}
