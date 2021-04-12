package com.blazingdb.calcite.optimizer.cost;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.*;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;

/**
 * 其底层是实现了 MetadataHandler<BuiltInMetadata.DistinctRowCount> 接口，用于提供 distinctRwoCount 信息
 */
public class CSVRelMdDistinctRowCount extends RelMdDistinctRowCount {

    private static final CSVRelMdDistinctRowCount INSTANCE =
            new CSVRelMdDistinctRowCount();

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
