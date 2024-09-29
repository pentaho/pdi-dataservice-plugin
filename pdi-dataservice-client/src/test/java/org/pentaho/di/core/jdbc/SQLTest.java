/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package org.pentaho.di.core.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;
import org.pentaho.di.core.exception.KettleSQLException;
import org.pentaho.di.core.sql.SQL;

public class SQLTest {
  @Test
  public void testExample1() throws KettleSQLException {
    String select = "A, B, C";
    String from = "Step";
    SQL sql = new SQL( "SELECT " + select + " FROM " + from );
    assertEquals( select, sql.getSelectClause() );
    assertEquals( from, sql.getServiceName() );
    assertNull( sql.getWhereClause() );
    assertNull( sql.getGroupClause() );
    assertNull( sql.getHavingClause() );
    assertNull( sql.getOrderClause() );
  }

  @Test
  public void testExample2() throws KettleSQLException {
    String select = "A, B, C";
    String from = "Step";
    String where = "D > 6 AND E = 'abcd'";
    SQL sql = new SQL( "SELECT " + select + " FROM " + from + " WHERE " + where );
    assertEquals( select, sql.getSelectClause() );
    assertEquals( from, sql.getServiceName() );
    assertEquals( where, sql.getWhereClause() );
    assertNull( sql.getGroupClause() );
    assertNull( sql.getHavingClause() );
    assertNull( sql.getOrderClause() );
  }

  @Test
  public void testExample3() throws KettleSQLException {
    String select = "A, B, C";
    String from = "Step";
    String order = "B, A, C";
    SQL sql = new SQL( "SELECT " + select + " FROM " + from + " ORDER BY " + order );
    assertEquals( select, sql.getSelectClause() );
    assertEquals( from, sql.getServiceName() );
    assertNull( sql.getWhereClause() );
    assertNull( sql.getGroupClause() );
    assertNull( sql.getHavingClause() );
    assertEquals( order, sql.getOrderClause() );
  }

  @Test
  public void testExample4() throws KettleSQLException {
    String select = "A, B, sum(C)";
    String from = "Step";
    String where = "D > 6 AND E = 'abcd'";
    String having = "sum(C) > 100";
    String order = "sum(C) DESC";
    SQL sql =
        new SQL( "SELECT " + select + " FROM " + from + " WHERE " + where + " HAVING " + having
            + " ORDER BY " + order );
    assertEquals( select, sql.getSelectClause() );
    assertEquals( from, sql.getServiceName() );
    assertEquals( where, sql.getWhereClause() );
    assertEquals( having, sql.getHavingClause() );
    assertEquals( order, sql.getOrderClause() );
  }

  @Test
  public void testExample5() throws KettleSQLException {
    String select = "A, B, sum(C)";
    String from = "Step";
    String where = "D > 6 AND E = 'abcd'";
    String group = "A, B";
    String having = "sum(C) > 100";
    String order = "sum(C) DESC";
    SQL sql =
        new SQL( "SELECT " + select + " FROM " + from + " WHERE " + where + " GROUP BY " + group + " HAVING " + having
            + " ORDER BY " + order );
    assertEquals( select, sql.getSelectClause() );
    assertEquals( from, sql.getServiceName() );
    assertEquals( where, sql.getWhereClause() );
    assertEquals( group, sql.getGroupClause() );
    assertEquals( having, sql.getHavingClause() );
    assertEquals( order, sql.getOrderClause() );
  }

  @Test
  public void testWhereInColumnIndexPDI12347() throws KettleSQLException {
    String select = "whereDoYouLive, good, fine";
    String from = "testingABC";
    SQL sql = new SQL( "SELECT " + select + " FROM " + from );
    assertEquals( select, sql.getSelectClause() );
    assertEquals( from, sql.getServiceName() );
    assertNull( sql.getWhereClause() );
    assertNull( sql.getGroupClause() );
    assertNull( sql.getHavingClause() );
    assertNull( sql.getOrderClause() );
  }
}
