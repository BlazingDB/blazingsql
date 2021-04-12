package com.blazingdb.calcite.optimizer.reloperators;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

public interface CSVRel extends RelNode {
    Convention CONVENTION = new Convention.Impl("CSV", CSVRel.class);
}
