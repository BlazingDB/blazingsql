package com.blazingdb.calcite.cbo.reloperators;

import com.blazingdb.calcite.optimizer.reloperators.CSVProject;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class LocalProject extends Project implements LocalRelNode {
    protected LocalProject(RelOptCluster cluster, RelTraitSet traits, List<RelHint> hints, RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
        super(cluster, traits, hints, input, projects, rowType);
    }

    public LocalProject(RelOptCluster cluster, RelTraitSet traits, RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
        super(cluster, traits, input, projects, rowType);
    }

    protected LocalProject(RelInput input) {
        super(input);
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return super.estimateRowCount(mq);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq);
    }

    @Override
    public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
        //NPE
        return new LocalProject(getCluster(),traitSet,input,projects,rowType);
    }

    @Override
    public RelNode attachHints(List<RelHint> hintList) {
        return null;
    }

    @Override
    public RelNode withHints(List<RelHint> hintList) {
        return null;
    }
}
