package com.blazingdb.calcite.optimizer.reloperators;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class NewCsvProject extends Project implements CSVRel {
    private RelOptCost cost;

    public NewCsvProject(RelOptCluster cluster, RelTraitSet traits, RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
        super(cluster, traits, input, projects, rowType);
    }

    @Override
    public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
        return new NewCsvProject(getCluster(),traitSet,input,projects,rowType);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double dRows = mq.getRowCount(getInput());

        double dCpu = dRows * exps.size();
        double dIo = 0;
//        return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
        //返回不同的Cost，CBO的结果不一样
        return planner.getCostFactory().makeCost(10, 10, 0);
//        return planner.getCostFactory().makeCost(40, 40, 0);

    }

}
