package com.blazingdb.calcite.application;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlSyntaxException extends SqlException {
	private static final long serialVersionUID = -1689099602920569510L;

	public SqlSyntaxException(final String queryString, final SqlParseException sqlParseException) {
		super(description(queryString, sqlParseException));
	}

	private static String
	description(final String queryString, final SqlParseException sqlParseException) {
		final StringBuilder builder = new StringBuilder();
		builder.append("SqlSyntaxException\n\n");

		final SqlParserPos pos = sqlParseException.getPos();
		SqlException.pointInQueryString(builder,
			queryString,
			new SqlPosition(pos.getLineNum(), pos.getColumnNum(), pos.getEndLineNum(), pos.getEndColumnNum()));

		builder.append('\n');
		builder.append(sqlParseException.getMessage());

		return builder.toString();
	}
}
