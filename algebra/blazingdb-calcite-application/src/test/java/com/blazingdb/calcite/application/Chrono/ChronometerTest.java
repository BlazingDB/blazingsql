package com.blazingdb.calcite.application.Chrono;

import static org.junit.Assert.*;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import org.junit.Test;

public class ChronometerTest {
	private final StubWatch watch = new StubWatch();
	private final Chronometer chronometer = new Chronometer(watch);

	@Test
	public void
	createUnstarted() {
		Chronometer chronometer = Chronometer.makeUnstarted();
		assertFalse(chronometer.isRunning());
		assertEquals(0, chronometer.elapsed());
	}

	@Test
	public void
	createStarted() {
		Chronometer chronometer = Chronometer.makeStarted();
		assertTrue(chronometer.isRunning());
	}

	@Test
	public void
	start() {
		assertFalse(chronometer.isRunning());
		assertEquals(0, chronometer.elapsed());
	}

	@Test
	public void
	startAgain() {
		chronometer.start();
		try {
			chronometer.start();
			fail();
		} catch(IllegalStateException expected) {
		}
		assertTrue(chronometer.isRunning());
	}

	@Test
	public void
	stop() {
		chronometer.start();
		assertSame(chronometer, chronometer.stop());
		assertFalse(chronometer.isRunning());
	}

	@Test
	public void
	stopCreated() {
		try {
			chronometer.stop();
			fail();
		} catch(IllegalStateException expected) {
		}
		assertFalse(chronometer.isRunning());
	}

	@Test
	public void
	stopAlreadyStopped() {
		chronometer.start();
		chronometer.stop();
		try {
			chronometer.stop();
			fail();
		} catch(IllegalStateException expected) {
		}
		assertFalse(chronometer.isRunning());
	}

	@Test
	public void
	timelineStart() {
		watch.add(3);
		chronometer.reset();
		assertFalse(chronometer.isRunning());
		watch.add(4);
		assertEquals(0, chronometer.elapsed());
		chronometer.start();
		watch.add(5);
		assertEquals(5, chronometer.elapsed());
	}

	@Test
	public void
	timelineAgain() {
		watch.add(3);
		chronometer.start();
		assertEquals(0, chronometer.elapsed());
		watch.add(4);
		assertEquals(4, chronometer.elapsed());
		chronometer.reset();
		assertFalse(chronometer.isRunning());
		watch.add(5);
		assertEquals(0, chronometer.elapsed());
	}

	@Test
	public void
	timelineElapsed() {
		watch.add(9);
		chronometer.start();
		assertEquals(0, chronometer.elapsed());
		watch.add(7);
		assertEquals(7, chronometer.elapsed());
	}

	@Test
	public void
	timelineNotRunning() {
		watch.add(5);
		chronometer.start();
		watch.add(6);
		chronometer.stop();
		watch.add(7);
		assertEquals(6, chronometer.elapsed());
	}

	@Test
	public void
	timelineReuse() {
		chronometer.start();
		watch.add(5);
		chronometer.stop();
		watch.add(6);
		chronometer.start();
		assertEquals(5, chronometer.elapsed());
		watch.add(7);
		assertEquals(12, chronometer.elapsed());
		chronometer.stop();
		watch.add(8);
		assertEquals(12, chronometer.elapsed());
	}

	@Test
	public void
	milliseconds() {
		chronometer.start();
		watch.add(999999);
		assertEquals(0, chronometer.elapsed(MILLISECONDS));
		watch.add(1);
		assertEquals(1, chronometer.elapsed(MILLISECONDS));
	}
}
