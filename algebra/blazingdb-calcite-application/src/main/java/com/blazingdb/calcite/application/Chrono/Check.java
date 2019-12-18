package com.blazingdb.calcite.application.Chrono;

public final class Check {
	public static void
	state(boolean expression) {
		if(!expression) {
			throw new IllegalStateException();
		}
	}
}
