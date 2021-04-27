package com.blazingdb.calcite.cbo.reloperators;

import com.blazingdb.calcite.cbo.stats.RelOptLocalTable;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import java.util.List;

public class LocalTableScan extends TableScan implements LocalRelNode {
    private RelOptLocalTable relOptLocalTable;
    protected LocalTableScan(RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints, RelOptTable table) {
        super(cluster, traitSet, hints, table);
    }

//    public LocalTableScan(RelOptLocalTable relOptLocalTable){
//        this.relOptLocalTable = relOptLocalTable;
//    }

    public LocalTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
        super(cluster, traitSet, table);
    }

    protected LocalTableScan(RelInput input) {
        super(input);
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return 13123;
    }

//    @Override
//    public double estimateRowCount(RelMetadataQuery mq) {
//        return ((RelOptLocalTable) table).getRowCount();
//    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq);
    }

    ////
    @Override
    public RelNode attachHints(List<RelHint> hintList) {
        return null;
    }

    @Override
    public RelNode withHints(List<RelHint> hintList) {
        return null;
    }
}
