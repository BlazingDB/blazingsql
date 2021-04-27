package com.blazingdb.calcite.optimizer.converter;
import com.blazingdb.calcite.optimizer.reloperators.CSVRel;
import com.blazingdb.calcite.optimizer.reloperators.CSVTableScan;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;


public class CSVTableScanConverter extends ConverterRule {

    public static final CSVTableScanConverter INSTANCE = new CSVTableScanConverter(
            LogicalTableScan.class,
            Convention.NONE,
            CSVRel.CONVENTION,
            "CSVTableScan"
    );

    @Override
    public boolean matches(RelOptRuleCall call) {
        return super.matches(call);
    }

    public CSVTableScanConverter(Class<? extends RelNode> clazz, RelTrait in, RelTrait out, String description) {
        super(clazz, in, out, description);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalTableScan tableScan = (LogicalTableScan) rel;
        return new CSVTableScan(tableScan.getCluster(),
                RelTraitSet.createEmpty().plus(CSVRel.CONVENTION).plus(RelDistributionTraitDef.INSTANCE.getDefault()),
                tableScan.getTable());
    }
}
