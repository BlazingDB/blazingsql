package com.blazingdb.calcite.application.Chrono;

public abstract class Watch {
	protected Watch() {}

	public abstract long
	read();

	public static Watch
	internalWatch() {
		return INTERNAL_WATCH;
	}

	private static final Watch INTERNAL_WATCH = new Watch() {
		@Override
		public long read() {
			return System.nanoTime();
		}
	};
}
