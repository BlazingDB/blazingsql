package com.blazingdb.calcite.application;

import java.nio.ByteBuffer;

public class ProcessRequestResult {
	public ByteBuffer responseBuffer;
	public boolean shutdown;

	ProcessRequestResult(ByteBuffer responseBuffer, boolean shutdown) {
		this.responseBuffer = responseBuffer;
		this.shutdown = shutdown;
	}
}
