package com.blazingdb.calcite.application;

import java.util.Arrays;
import java.util.List;

public class SqlException extends Exception {
	private static final long serialVersionUID = 8622591427008623844L;

	public SqlException(final String message) { super(message); }

	public static void
	pointInQueryString(final StringBuilder builder, final String queryString, final SqlPosition sqlPosition) {
		final List<String> queryLines = Arrays.asList(queryString.split("\n"));

		// append unaffected lines
		for(int i = 0; i < sqlPosition.getLineNum(); i++) {
			builder.append(queryLines.get(i));
			builder.append('\n');
		}

		// append lines with syntax error
		for(int i = 1; i < sqlPosition.getColumnNum(); i++) {
			builder.append(' ');
		}
		for(int i = sqlPosition.getColumnNum(); i <= sqlPosition.getEndColumnNum(); i++) {
			builder.append('^');
		}
		builder.append('\n');

		// append rest of lines
		for(int i = sqlPosition.getEndLineNum(); i < queryLines.size(); i++) {
			builder.append(queryLines.get(i));
			builder.append('\n');
		}
	}

	static final class SqlPosition {
		private final int lineNum;
		private final int columnNum;
		private final int endLineNum;
		private final int endColumnNum;

		public SqlPosition(final int lineNum, final int columnNum, final int endLineNum, final int endColumnNum) {
			this.lineNum = lineNum;
			this.columnNum = columnNum;
			this.endLineNum = endLineNum;
			this.endColumnNum = endColumnNum;
		}

		int
		getLineNum() {
			return lineNum;
		}
		int
		getColumnNum() {
			return columnNum;
		}
		int
		getEndLineNum() {
			return endLineNum;
		}
		int
		getEndColumnNum() {
			return endColumnNum;
		}
	}
}
