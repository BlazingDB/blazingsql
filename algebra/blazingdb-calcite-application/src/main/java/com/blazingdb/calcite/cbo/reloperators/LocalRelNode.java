package com.blazingdb.calcite.cbo.reloperators;

import com.blazingdb.calcite.optimizer.reloperators.CSVRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

public interface LocalRelNode extends RelNode {
    Convention CONVENTION = new Convention.Impl("LocalCBO", LocalRelNode.class);

}
