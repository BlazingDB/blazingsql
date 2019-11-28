/*
 * Copyright 2018 BlazingDB, Inc.
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

package com.blazingdb.calcite.sql.validate;

import org.apache.calcite.sql.validate.SqlAbstractConformance;

public class BlazingSqlConformance extends SqlAbstractConformance {

	@Override
	public boolean isPercentRemainderAllowed() {
		return true;
	}

}
