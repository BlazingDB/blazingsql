package com.blazingdb.calcite.cbo.converter;

import com.blazingdb.calcite.cbo.reloperators.LocalFilter;
import com.blazingdb.calcite.cbo.reloperators.LocalRelNode;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

public class LocalFilterConverter extends ConverterRule {
    public static final LocalFilterConverter INSTANCE = new LocalFilterConverter(
            LogicalFilter.class,
            Convention.NONE,
            LocalRelNode.CONVENTION,
            "LocalFilterConverter"
    );

    public LocalFilterConverter(Class<? extends RelNode> clazz, RelTrait in, RelTrait out, String descriptionPrefix) {
        super(clazz, in, out, descriptionPrefix);
    }

    public <R extends RelNode> LocalFilterConverter(Class<R> clazz, Predicate<? super R> predicate, RelTrait in, RelTrait out, RelBuilderFactory relBuilderFactory, String descriptionPrefix) {
        super(clazz, predicate, in, out, relBuilderFactory, descriptionPrefix);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return super.matches(call);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalFilter filter = (LogicalFilter) rel;
        RelNode input = convert(filter.getInput(), filter.getInput().getTraitSet().replace(LocalRelNode.CONVENTION).simplify());
        return new LocalFilter(
                filter.getCluster(),
                RelTraitSet.createEmpty().plus(LocalRelNode.CONVENTION).plus(RelDistributionTraitDef.INSTANCE.getDefault()),
                input,
                filter.getCondition()
        );
    }
}
