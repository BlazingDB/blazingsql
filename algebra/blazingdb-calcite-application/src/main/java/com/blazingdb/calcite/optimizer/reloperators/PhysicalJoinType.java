package com.blazingdb.calcite.optimizer.reloperators;

/**
 * @author yuqi
 * @mail yuqi5@xiaomi.com
 * @description your description
 * @time 2/3/21 下午8:45
 **/
public enum PhysicalJoinType {

    /**
     *
     */
    NONE("None", 0),
    /**
     *
     */
    NEST_LOOP_JOIN("NestLoopJoin", 1),

    /**
     *
     */
    HASH_JOIN("HashJoin", 2),


    /**
     *
     */
    SORT_MERGE("SortMerge", 3);

    private final String name;
    private final int index;

    PhysicalJoinType(String name, int index) {
        this.name = name;
        this.index = index;
    }
}
