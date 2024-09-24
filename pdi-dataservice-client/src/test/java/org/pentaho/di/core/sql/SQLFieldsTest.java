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

package org.pentaho.di.core.sql;

import junit.framework.TestCase;

import org.pentaho.di.core.exception.KettleSQLException;
import org.pentaho.di.core.row.RowMetaInterface;

public class SQLFieldsTest extends TestCase {

  public void testSqlFromFields01() throws KettleSQLException {
    RowMetaInterface rowMeta = SQLTest.generateTest2RowMeta();

    String fieldsClause = "A, B";

    SQLFields fromFields = new SQLFields( "Service", rowMeta, fieldsClause );
    assertFalse( fromFields.isDistinct() );

    assertEquals( 2, fromFields.getFields().size() );

    SQLField field = fromFields.getFields().get( 0 );
    assertEquals( "A", field.getName() );
    assertNull( field.getAlias() );
    assertNotNull( "The service data type was not discovered", field.getValueMeta() );
    assertEquals( "A", field.getValueMeta().getName().toUpperCase() );

    field = fromFields.getFields().get( 1 );
    assertEquals( "B", field.getName() );
    assertNull( field.getAlias() );
    assertNotNull( "The service data type was not discovered", field.getValueMeta() );
    assertEquals( "B", field.getValueMeta().getName().toUpperCase() );
  }

  public void testSqlFromFields02() throws KettleSQLException {
    RowMetaInterface rowMeta = SQLTest.generateTest2RowMeta();

    String fieldsClause = "A as foo, B";

    SQLFields fromFields = new SQLFields( "Service", rowMeta, fieldsClause );
    assertFalse( fromFields.isDistinct() );

    assertEquals( 2, fromFields.getFields().size() );

    SQLField field = fromFields.getFields().get( 0 );
    assertEquals( "A", field.getName() );
    assertEquals( "foo", field.getAlias() );
    assertNotNull( "The service data type was not discovered", field.getValueMeta() );
    assertEquals( "A", field.getValueMeta().getName().toUpperCase() );

    field = fromFields.getFields().get( 1 );
    assertEquals( "B", field.getName() );
    assertNull( field.getAlias() );
    assertNotNull( "The service data type was not discovered", field.getValueMeta() );
    assertEquals( "B", field.getValueMeta().getName().toUpperCase() );
  }

  public void testSqlFromFields03() throws KettleSQLException {
    RowMetaInterface rowMeta = SQLTest.generateTest2RowMeta();

    String fieldsClause = "A, sum( B ) as \"Total sales\"";

    SQLFields fromFields = new SQLFields( "Service", rowMeta, fieldsClause );
    assertFalse( fromFields.isDistinct() );

    assertEquals( 2, fromFields.getFields().size() );

    SQLField field = fromFields.getFields().get( 0 );
    assertEquals( "A", field.getName() );
    assertNull( field.getAlias() );
    assertNotNull( "The service data type was not discovered", field.getValueMeta() );
    assertEquals( "A", field.getValueMeta().getName().toUpperCase() );

    field = fromFields.getFields().get( 1 );
    assertEquals( "B", field.getName() );
    assertEquals( "Total sales", field.getAlias() );
    assertNotNull( "The service data type was not discovered", field.getValueMeta() );
    assertEquals( "B", field.getValueMeta().getName().toUpperCase() );
  }

  public void testSqlFromFields04() throws KettleSQLException {
    RowMetaInterface rowMeta = SQLTest.generateTest2RowMeta();

    String fieldsClause = "DISTINCT A as foo, B as bar";

    SQLFields fromFields = new SQLFields( "Service", rowMeta, fieldsClause );
    assertTrue( fromFields.isDistinct() );

    assertEquals( 2, fromFields.getFields().size() );

    SQLField field = fromFields.getFields().get( 0 );
    assertEquals( "A", field.getName() );
    assertEquals( "foo", field.getAlias() );
    assertNotNull( "The service data type was not discovered", field.getValueMeta() );
    assertEquals( "A", field.getValueMeta().getName().toUpperCase() );

    field = fromFields.getFields().get( 1 );
    assertEquals( "B", field.getName() );
    assertEquals( "bar", field.getAlias() );
    assertNotNull( "The service data type was not discovered", field.getValueMeta() );
    assertEquals( "B", field.getValueMeta().getName().toUpperCase() );
  }

  public void testSqlFromFields05() throws KettleSQLException {
    RowMetaInterface rowMeta = SQLTest.generateTest2RowMeta();

    String fieldsClause = "*";

    SQLFields fromFields = new SQLFields( "Service", rowMeta, fieldsClause );
    assertFalse( fromFields.isDistinct() );

    assertEquals( 2, fromFields.getFields().size() );

    SQLField field = fromFields.getFields().get( 0 );
    assertEquals( "A", field.getName() );
    assertNull( field.getAlias() );
    assertNotNull( "The service data type was not discovered", field.getValueMeta() );
    assertEquals( "A", field.getValueMeta().getName().toUpperCase() );

    field = fromFields.getFields().get( 1 );
    assertEquals( "B", field.getName() );
    assertNull( field.getAlias() );
    assertNotNull( "The service data type was not discovered", field.getValueMeta() );
    assertEquals( "B", field.getValueMeta().getName().toUpperCase() );
  }

  public void testSqlFromFields05Alias() throws KettleSQLException {
    RowMetaInterface rowMeta = SQLTest.generateTest2RowMeta();

    String fieldsClause = "Service.*";

    SQLFields fromFields = new SQLFields( "Service", rowMeta, fieldsClause );
    assertFalse( fromFields.isDistinct() );

    assertEquals( 2, fromFields.getFields().size() );

    SQLField field = fromFields.getFields().get( 0 );
    assertEquals( "A", field.getName() );
    assertNull( field.getAlias() );
    assertNotNull( "The service data type was not discovered", field.getValueMeta() );
    assertEquals( "A", field.getValueMeta().getName().toUpperCase() );

    field = fromFields.getFields().get( 1 );
    assertEquals( "B", field.getName() );
    assertNull( field.getAlias() );
    assertNotNull( "The service data type was not discovered", field.getValueMeta() );
    assertEquals( "B", field.getValueMeta().getName().toUpperCase() );
  }

  public void testSqlFromFields06() throws KettleSQLException {
    RowMetaInterface rowMeta = SQLTest.generateTest2RowMeta();

    String fieldsClause = "*, *";

    SQLFields fromFields = new SQLFields( "Service", rowMeta, fieldsClause );
    assertFalse( fromFields.isDistinct() );

    assertEquals( 4, fromFields.getFields().size() );

    SQLField field = fromFields.getFields().get( 0 );
    assertEquals( "A", field.getName() );
    assertNull( field.getAlias() );
    assertNotNull( "The service data type was not discovered", field.getValueMeta() );
    assertEquals( "A", field.getValueMeta().getName().toUpperCase() );

    field = fromFields.getFields().get( 1 );
    assertEquals( "B", field.getName() );
    assertNull( field.getAlias() );
    assertNotNull( "The service data type was not discovered", field.getValueMeta() );
    assertEquals( "B", field.getValueMeta().getName().toUpperCase() );

    field = fromFields.getFields().get( 2 );
    assertEquals( "A", field.getName() );
    assertNull( field.getAlias() );
    assertNotNull( "The service data type was not discovered", field.getValueMeta() );
    assertEquals( "A", field.getValueMeta().getName().toUpperCase() );

    field = fromFields.getFields().get( 3 );
    assertEquals( "B", field.getName() );
    assertNull( field.getAlias() );
    assertNotNull( "The service data type was not discovered", field.getValueMeta() );
    assertEquals( "B", field.getValueMeta().getName().toUpperCase() );
  }
}
