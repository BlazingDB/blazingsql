/*
 * This file is a copy with just one modification of the TransformationRule from
 * the Apache Calcite project. The original code can be found at:
 * https://github.com/apache/calcite/blob/branch-1.23/core/src/main/java/org/apache/calcite/rel/rules/TransformationRule.java
 * The change is about the current package from blazingdb
 */

package com.blazingdb.calcite.rules;

import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.PhysicalNode;

/**
 * Logical transformation rule, only logical operator can be rule operand,
 * and only generate logical alternatives. It is only visible to
 * {@link VolcanoPlanner}, {@link HepPlanner} will ignore this interface.
 * That means, in {@link HepPlanner}, the rule that implements
 * {@link TransformationRule} can still match with physical operator of
 * {@link PhysicalNode} and generate physical alternatives.
 *
 * <p>But in {@link VolcanoPlanner}, {@link TransformationRule} doesn't match
 * with physical operator that implements {@link PhysicalNode}. It is not
 * allowed to generate physical operators in {@link TransformationRule},
 * unless you are using it in {@link HepPlanner}.</p>
 *
 * @see VolcanoPlanner
 * @see SubstitutionRule
 */
public interface TransformationRule {
}
