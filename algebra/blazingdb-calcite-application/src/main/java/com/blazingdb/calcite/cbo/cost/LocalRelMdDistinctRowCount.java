package com.blazingdb.calcite.cbo.cost;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.*;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;

public class LocalRelMdDistinctRowCount extends RelMdDistinctRowCount {

    private static final LocalRelMdDistinctRowCount INSTANCE =
            new LocalRelMdDistinctRowCount();

    public static final RelMetadataProvider SOURCE = ChainedRelMetadataProvider
            .of(ImmutableList.of(

                    ReflectiveRelMetadataProvider.reflectiveSource(
                            BuiltInMethod.DISTINCT_ROW_COUNT.method, INSTANCE)));

//                    ReflectiveRelMetadataProvider.reflectiveSource(
//                            BuiltInMethod.CUMULATIVE_COST.method, INSTANCE)));

    @Override
    public Double getDistinctRowCount(RelNode rel, RelMetadataQuery mq, ImmutableBitSet groupKey,
                                      RexNode predicate) {

//            return getDistinctRowCount( rel, mq, groupKey, predicate);

        /*
         * For now use Calcite' default formulas for propagating NDVs up the Query
         * Tree.
         */
        return super.getDistinctRowCount(rel, mq, groupKey, predicate);
    }
}
