package com.blazingdb.calcite.cbo.model;

import org.apache.hadoop.hive.ql.plan.Statistics;

/*
What Statistics are collected?
The following statistics currently supported for table and partitions:

Number of rows
Number of files
Size in Bytes
Number of partition if the table is partitioned
 */
public class LocalStatistics extends Statistics {
}
