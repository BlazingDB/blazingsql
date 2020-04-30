package com.blazingdb.calcite.application;

import org.apache.calcite.tools.ValidationException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlValidationException extends SqlException {
	private static final long serialVersionUID = -429299379494895888L;

	public SqlValidationException(final String queryString, final ValidationException validationException) {
		super(description(queryString, validationException.getMessage()));
	}

	private static Pattern pattern =
		Pattern.compile("From line (\\d+), column (\\d+) to line (\\d+), column (\\d+): .*");

	private static String
	description(final String queryString, final String message) {

		return message;

	}
}
