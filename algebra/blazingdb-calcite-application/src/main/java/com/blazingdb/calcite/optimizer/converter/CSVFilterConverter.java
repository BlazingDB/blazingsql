package com.blazingdb.calcite.optimizer.converter;

import com.blazingdb.calcite.optimizer.reloperators.CSVFilter;
import com.blazingdb.calcite.optimizer.reloperators.CSVRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalFilter;

public class CSVFilterConverter extends ConverterRule {

    public static final CSVFilterConverter INSTANCE = new CSVFilterConverter(
            LogicalFilter.class,
            Convention.NONE,
            CSVRel.CONVENTION,
            "CSVFilterConverter"
    );
    public CSVFilterConverter(Class<? extends RelNode> clazz, RelTrait in, RelTrait out, String description) {
        super(clazz, in, out, description);
    }

    /**
     * RelOptRuleCall 是专门用来被RelOptRule调用的，包含一个 RelNode 的集合 （Set）。
     * @param call
     * @return
     */
    @Override
    public boolean matches(RelOptRuleCall call) {
        return super.matches(call);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalFilter filter = (LogicalFilter) rel;
        RelNode input = convert(filter.getInput(), filter.getInput().getTraitSet().replace(CSVRel.CONVENTION).simplify());
        return new CSVFilter(
                filter.getCluster(),
                RelTraitSet.createEmpty().plus(CSVRel.CONVENTION).plus(RelDistributionTraitDef.INSTANCE.getDefault()),
                input,
                filter.getCondition()
        );
    }
}
