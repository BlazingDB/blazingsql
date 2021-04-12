package com.blazingdb.calcite.optimizer.converter;

import com.blazingdb.calcite.optimizer.reloperators.CSVProject;
import com.blazingdb.calcite.optimizer.reloperators.CSVRel;
import com.blazingdb.calcite.optimizer.reloperators.NewCsvProject;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilderFactory;


public class CSVNewProjectRule extends RelOptRule {
    public static final CSVNewProjectRule INSTANCE =
            new CSVNewProjectRule(RelFactories.LOGICAL_BUILDER);

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a ProjectRemoveRule.
     *
     * @param relBuilderFactory Builder for relational expressions
     */
    public CSVNewProjectRule(RelBuilderFactory relBuilderFactory) {
        // Create a specialized operand to detect non-matches early. This keeps
        // the rule queue short.
        super(operandJ(Project.class, null, CSVNewProjectRule::isTrivial, any()),
                relBuilderFactory, null);
    }

    //~ Methods ----------------------------------------------------------------

    public void onMatch(RelOptRuleCall call) {
        Project project = call.rel(0);
        assert isTrivial(project);
        RelNode stripped = project.getInput();

        if (project instanceof CSVProject) {
            CSVProject csvProject = (CSVProject) project;
            NewCsvProject newCsvProject = new NewCsvProject(
                    csvProject.getCluster(),
                    RelTraitSet.createEmpty().plus(CSVRel.CONVENTION).plus(RelDistributionTraitDef.INSTANCE.getDefault()),
                    csvProject.getInput(),
                    csvProject.getProjects(),
                    csvProject.getRowType()
            );

            call.transformTo(newCsvProject);
        }
    }

    /**
     * Returns the child of a project if the project is trivial, otherwise
     * the project itself.
     */
    public static RelNode strip(Project project) {
        CSVProject csvProject = (CSVProject) project;
        RelNode input = convert(csvProject.getInput(), csvProject.getInput().getTraitSet().replace(CSVRel.CONVENTION).simplify());
        return new NewCsvProject(
                csvProject.getCluster(),
                RelTraitSet.createEmpty().plus(CSVRel.CONVENTION).plus(RelDistributionTraitDef.INSTANCE.getDefault()),
                input,
                csvProject.getProjects(),
                csvProject.getRowType()
        );
    }

    public static boolean isTrivial(Project project) {
        return RexUtil.isIdentity(project.getProjects(),
                project.getInput().getRowType());
    }

//    @Deprecated // to be removed before 1.5
//    public static boolean isIdentity(List<? extends RexNode> exps,
//                                     RelDataType childRowType) {
//        return RexUtil.isIdentity(exps, childRowType);
//    }
}
