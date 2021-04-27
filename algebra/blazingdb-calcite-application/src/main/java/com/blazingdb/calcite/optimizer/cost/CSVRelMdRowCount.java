package com.blazingdb.calcite.optimizer.cost;

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

public class CSVRelMdRowCount extends RelMdRowCount {
    public MetadataDef<BuiltInMetadata.RowCount> getDef() {
        return BuiltInMetadata.RowCount.DEF;
    }

    public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider
            .reflectiveSource(BuiltInMethod.ROW_COUNT.method, new CSVRelMdRowCount());

    @Override
    public Double getRowCount(RelNode rel, RelMetadataQuery mq) {
        return 1.0;
    }

    //以下两个方法用于测试 CBO 优化
    public Double getRowCount(NewCsvProject rel, RelMetadataQuery mq) {
        return 167.0;
    }

//    // 2do usarra este mas alto nivel y generico
//    @Override
//    public Double getRowCount(TableScan rel, RelMetadataQuery mq) {
//        return 2354.34;
//    }

    // 1ro usaria este mas especifico
//    public Double getRowCount(CSVTableScan rel, RelMetadataQuery mq) {
//        return 12.78;
//    }

    public Double getRowCount(CSVProject rel, RelMetadataQuery mq) {
        return 1.0;
    }

    @Override
    public Double getRowCount(Project rel, RelMetadataQuery mq) {
        return 2.0;
    }

    //实现自己的 cost 计算逻辑
    @Override
    public Double getRowCount(Filter rel, RelMetadataQuery mq) {
        return mq.getRowCount(rel.getInput()) / 5;


//        return 2234.0;
    }


    public Double getRowCount(RelNode rel) {
        return 1.0;
    }

    public Double getRowCount(RelNode rel,RexNode rex,RelMetadataQuery mq) {
        return 1.0;
    }
}
