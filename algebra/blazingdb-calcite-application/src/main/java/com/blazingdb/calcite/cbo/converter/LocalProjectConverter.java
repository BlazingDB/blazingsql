package com.blazingdb.calcite.cbo.converter;

import com.blazingdb.calcite.cbo.reloperators.LocalProject;
import com.blazingdb.calcite.cbo.reloperators.LocalRelNode;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

public class LocalProjectConverter extends ConverterRule {
    public static final LocalProjectConverter INSTANCE = new LocalProjectConverter(
            LogicalProject.class,
            Convention.NONE,
            LocalRelNode.CONVENTION,
            "LocalProjectConverter"
    );

    public LocalProjectConverter(Class<? extends RelNode> clazz, RelTrait in, RelTrait out, String descriptionPrefix) {
        super(clazz, in, out, descriptionPrefix);
    }

    public <R extends RelNode> LocalProjectConverter(Class<R> clazz, Predicate<? super R> predicate, RelTrait in, RelTrait out, RelBuilderFactory relBuilderFactory, String descriptionPrefix) {
        super(clazz, predicate, in, out, relBuilderFactory, descriptionPrefix);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return super.matches(call);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalProject logicalProject = (LogicalProject) rel;
        RelNode input = convert(logicalProject.getInput(), logicalProject.getInput().getTraitSet().replace(LocalRelNode.CONVENTION).simplify());
        return new LocalProject(
                logicalProject.getCluster(),
                RelTraitSet.createEmpty().plus(LocalRelNode.CONVENTION).plus(RelDistributionTraitDef.INSTANCE.getDefault()),
                input,
                logicalProject.getProjects(),
                logicalProject.getRowType()
        );
    }
}
