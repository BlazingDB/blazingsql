package com.blazingdb.calcite.application.Chrono;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class StubWatch extends Watch {

  private final AtomicLong time = new AtomicLong();
  private volatile long step;

  public StubWatch add(final long increment, final TimeUnit timeUnit) {
    return add(timeUnit.toNanos(increment));
  }

  public StubWatch add(final long increment) {
    time.addAndGet(increment);
    return this;
  }

  public StubWatch setStep(final long value, TimeUnit timeUnit) {
    step = timeUnit.toNanos(value);
    return this;
  }

  @Override
  public long read() {
    return time.getAndAdd(step);
  }
}
