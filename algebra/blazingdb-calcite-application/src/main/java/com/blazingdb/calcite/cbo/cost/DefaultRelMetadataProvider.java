package com.blazingdb.calcite.cbo.cost;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;

public class DefaultRelMetadataProvider {

    public RelMetadataProvider getMetadataProvider() {
        // Return MD provider
        return ChainedRelMetadataProvider.of(ImmutableList
                .of(
                        LocalRelMdRowCount.SOURCE,
                        LocalRelMdDistinctRowCount.SOURCE,
                        org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE
                )
        );
    }
}
