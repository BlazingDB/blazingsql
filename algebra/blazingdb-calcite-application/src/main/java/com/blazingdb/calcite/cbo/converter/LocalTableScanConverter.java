package com.blazingdb.calcite.cbo.converter;

import com.blazingdb.calcite.cbo.reloperators.LocalRelNode;
import com.blazingdb.calcite.cbo.reloperators.LocalTableScan;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

public class LocalTableScanConverter extends ConverterRule {
    public static final LocalTableScanConverter INSTANCE = new LocalTableScanConverter(
            LogicalTableScan.class,
            Convention.NONE,
            LocalRelNode.CONVENTION,
            "LocalTableScan"
    );

    public LocalTableScanConverter(Class<? extends RelNode> clazz, RelTrait in, RelTrait out, String descriptionPrefix) {
        super(clazz, in, out, descriptionPrefix);
    }


    public <R extends RelNode> LocalTableScanConverter(Class<R> clazz, Predicate<? super R> predicate, RelTrait in, RelTrait out, RelBuilderFactory relBuilderFactory, String descriptionPrefix) {
        super(clazz, predicate, in, out, relBuilderFactory, descriptionPrefix);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return super.matches(call);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalTableScan tableScan = (LogicalTableScan) rel;
        return new LocalTableScan(tableScan.getCluster(),
                RelTraitSet.createEmpty().plus(LocalRelNode.CONVENTION).plus(RelDistributionTraitDef.INSTANCE.getDefault()),
                tableScan.getTable());
    }
}
