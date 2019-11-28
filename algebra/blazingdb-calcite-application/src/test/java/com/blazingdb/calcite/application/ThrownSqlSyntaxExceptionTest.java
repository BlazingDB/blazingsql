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
public class ThrownSqlSyntaxExceptionTest extends GetRelationalAlgebraTest {

  public ThrownSqlSyntaxExceptionTest(final String queryString,
                                      final String expectedMessage) {
    super(queryString, expectedMessage);
  }

  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {"select * from heroes whera age=1",
         "Encountered \"age\" at line 1, column 28."},
        {"select *\n  fram heroes\n  whera age=1\n  limit 1",
         "Encountered \"heroes\" at line 2, column 8."},
    });
  }

  @Before
  @Override
  public void SetUp() {
    super.SetUp();
  }

  @After
  @Override
  public void TearDown() {
    super.TearDown();
  }

  @Test(expected = SqlSyntaxException.class)
  @Override
  public void throwSqlException()
      throws SqlSyntaxException, SqlValidationException,
             RelConversionException {
    super.throwSqlException();
  }

  @Test
  @Override
  public void hasStartErrorPositionInMessage()
      throws SqlSyntaxException, SqlValidationException,
             RelConversionException {
    super.hasStartErrorPositionInMessage(SqlSyntaxException.class);
  }
}
