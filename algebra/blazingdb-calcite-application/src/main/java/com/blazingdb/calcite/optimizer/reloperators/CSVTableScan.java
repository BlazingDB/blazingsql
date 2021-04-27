package com.blazingdb.calcite.optimizer.reloperators;

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveRelNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;

import java.util.List;

public class CSVTableScan extends TableScan implements CSVRel {
    private RelOptCost cost;
//    /**
//     * RelOptCluster：palnner 运行时的环境，保存上下文信息
//     * RelTrait：用来定义逻辑表的物理相关属性（physical property），三种主要的 trait 类型是：Convention、RelCollation、RelDistribution；
//     * RelOpt：代表关系表
//     */
//    public CSVTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptLocalTable table) {
//        super(cluster, traitSet, table);
//    }

    public CSVTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
        super(cluster, traitSet, table);
    }

    @Override
    public void register(RelOptPlanner planner) {

    }

    // 3ro va a b uscar en su propio table scan
    @Override public double estimateRowCount(RelMetadataQuery mq) {
        //revisar
        return 1212;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        //return super.computeSelfCo(planner, mq);

        double dRows = table.getRowCount();

        if (cost != null) {
            return cost;
        }
        //通过工厂生成 RelOptCost ，注入自定义 cost 值并返回
        cost = planner.getCostFactory().makeCost(1, 1, 0);
        return cost;
    }

    /**
     * Copy TableScan operator with a new Row Schema. The new Row Schema can only
     * be a subset of this TS schema.
     *
     * @param newRowtype
     * @return
     */
//    public CSVTableScan copy(RelDataType newRowtype) {
//        return new CSVTableScan(getCluster(), getTraitSet(), ((RelOptLocalTable) table), this.tblAlias, this.concatQbIDAlias,
//                newRowtype, this.useQBIdInDigest, this.insideView);
//    }

}
