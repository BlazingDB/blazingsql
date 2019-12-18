package com.blazingdb.calcite.application.Chrono;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.util.concurrent.TimeUnit;

public class Chronometer {
	private final Watch watch;
	private boolean isRunning;
	private long startTime;
	private long elapsedTime;

	public static Chronometer
	makeUnstarted() {
		return new Chronometer();
	}

	public static Chronometer
	makeStarted() {
		return makeUnstarted().start();
	}

	Chronometer() { this.watch = Watch.internalWatch(); }

	Chronometer(Watch watch) { this.watch = watch; }

	public boolean
	isRunning() {
		return isRunning;
	}

	public Chronometer
	start() {
		Check.state(!isRunning);
		isRunning = true;
		startTime = watch.read();
		return this;
	}

	public Chronometer
	stop() {
		long stopTime = watch.read();
		Check.state(isRunning);
		isRunning = false;
		elapsedTime += stopTime - startTime;
		return this;
	}

	public long
	elapsed() {
		return isRunning ? watch.read() - startTime + elapsedTime : elapsedTime;
	}

	public long
	elapsed(TimeUnit timeUnit) {
		return timeUnit.convert(elapsed(), NANOSECONDS);
	}

	public Chronometer
	reset() {
		isRunning = false;
		elapsedTime = 0;
		return this;
	}
}
