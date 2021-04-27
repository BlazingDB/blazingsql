package com.blazingdb.calcite.cbo.cost;

import com.blazingdb.calcite.cbo.reloperators.LocalTableScan;
import com.blazingdb.calcite.optimizer.reloperators.CSVProject;
import com.blazingdb.calcite.optimizer.reloperators.CSVTableScan;
import com.blazingdb.calcite.optimizer.reloperators.NewCsvProject;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.*;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;

public class LocalRelMdRowCount extends RelMdRowCount {
    public MetadataDef<BuiltInMetadata.RowCount> getDef() {
        return BuiltInMetadata.RowCount.DEF;
    }

    public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider
            .reflectiveSource(BuiltInMethod.ROW_COUNT.method, new LocalRelMdRowCount());

//    // 2do usarra este mas alto nivel y generico
//    @Override
//    public Double getRowCount(TableScan rel, RelMetadataQuery mq) {
//        return 2354.34;
//    }

//     1ro usaria este mas especifico
    public Double getRowCount(LocalTableScan rel, RelMetadataQuery mq) {
        return 12.78;
    }

}
