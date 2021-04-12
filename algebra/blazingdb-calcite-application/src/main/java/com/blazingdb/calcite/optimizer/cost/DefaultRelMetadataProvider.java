package com.blazingdb.calcite.optimizer.cost;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;

public class DefaultRelMetadataProvider {

    public RelMetadataProvider getMetadataProvider() {
        // Return MD provider
        return ChainedRelMetadataProvider.of(ImmutableList
                .of(
                        CSVRelMdRowCount.SOURCE,
                        CSVRelMdDistinctRowCount.SOURCE,
                        org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE
                )
        );
    }
}
