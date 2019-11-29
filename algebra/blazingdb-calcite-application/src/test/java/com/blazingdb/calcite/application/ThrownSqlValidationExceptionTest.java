package com.blazingdb.calcite.application;

import org.apache.calcite.tools.RelConversionException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class ThrownSqlValidationExceptionTest extends GetRelationalAlgebraTest {
	public ThrownSqlValidationExceptionTest(final String queryString, final String expectedMessage) {
		super(queryString, expectedMessage);
	}

	@Parameters
	public static Collection<Object[]>
	data() {
		return Arrays.asList(new Object[][] {
			{"select * from heroes where agee=1", "From line 1, column 28 to line 1, column 31"},
			{"select nombre\n  from heroes\n  where age=1\n  limit 1", "From line 1, column 8 to line 1, column 13"},
		});
	}

	@Before
	@Override
	public void
	SetUp() {
		super.SetUp();
	}

	@After
	@Override
	public void
	TearDown() {
		super.TearDown();
	}

	@Test(expected = SqlValidationException.class)
	@Override
	public void
	throwSqlException() throws SqlSyntaxException, SqlValidationException, RelConversionException {
		super.throwSqlException();
	}

	@Test
	@Override
	public void
	hasStartErrorPositionInMessage() throws SqlSyntaxException, SqlValidationException, RelConversionException {
		super.hasStartErrorPositionInMessage(SqlValidationException.class);
	}
}
