package com.blazingdb.calcite.optimizer.reloperators;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

public class CSVFilter extends Filter implements CSVRel {
    private RelOptCost cost;

    public CSVFilter(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
        super(cluster, traits, child, condition);
    }

    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new CSVFilter(getCluster(),this.traitSet,input,condition);
    }


    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
		RelNode input = this.input;

//		RelOptCost inputCost;
//		if (input instanceof RelSubset) {
//			inputCost = ((RelSubset) input).
//		}
//
//		RelOptCost inputCost = mq.getCumulativeCost(this.input);


        //return mq.getCumulativeCost(this);
        //return VolcanoCost.FACTORY.makeZeroCost();
        double dRows = mq.getRowCount(this);
//        double dCpu = mq.getRowCount(getInput());
        double dCpu = dRows;

        double dIo = 0;
//        return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
        return planner.getCostFactory().makeCost(2, 2, 2);


    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return 12;
    }
}
