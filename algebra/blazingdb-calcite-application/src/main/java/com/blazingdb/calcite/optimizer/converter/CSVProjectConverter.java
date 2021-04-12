package com.blazingdb.calcite.optimizer.converter;

import com.blazingdb.calcite.optimizer.reloperators.CSVProject;
import com.blazingdb.calcite.optimizer.reloperators.CSVRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalProject;

public class CSVProjectConverter extends ConverterRule {

    public static final CSVProjectConverter INSTANCE = new CSVProjectConverter(
            LogicalProject.class,
            Convention.NONE,
            CSVRel.CONVENTION,
            "CSVProjectConverter"
    );

    public CSVProjectConverter(Class<? extends RelNode> clazz, RelTrait in, RelTrait out, String description) {
        super(clazz, in, out, description);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return super.matches(call);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalProject logicalProject = (LogicalProject) rel;
        RelNode input = convert(logicalProject.getInput(), logicalProject.getInput().getTraitSet().replace(CSVRel.CONVENTION).simplify());
        return new CSVProject(
                logicalProject.getCluster(),
                RelTraitSet.createEmpty().plus(CSVRel.CONVENTION).plus(RelDistributionTraitDef.INSTANCE.getDefault()),
                input,
                logicalProject.getProjects(),
                logicalProject.getRowType()
        );
    }
}
