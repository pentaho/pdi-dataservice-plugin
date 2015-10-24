/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.dataservice.jdbc;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.value.ValueMetaBoolean;
import org.pentaho.di.core.row.value.ValueMetaDate;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaNumber;
import org.pentaho.di.core.row.value.ValueMetaString;

import java.sql.SQLException;
import java.util.Date;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class ThinResultSetTest extends JDBCTestBase<ThinResultSet> {

  public static final String INVALID_COLUMN_REFERENCE = "Invalid column reference: ";
  RowMeta rowMeta;
  ThinResultSet thinResultSet;

  public ThinResultSetTest() {
    super( ThinResultSet.class );
  }

  @Before
  public void setUp() throws Exception {
    thinResultSet = new ThinResultSet( mock( ThinStatement.class ) );
    thinResultSet.rowMeta = rowMeta = new RowMeta();
  }

  @Test public void testGetBoolean() throws Exception {
    thinResultSet.currentRow = new Object[] { false, "foo" };
    rowMeta.addValueMeta( new ValueMetaBoolean( "bool" ) );
    rowMeta.addValueMeta( new ValueMetaString( "string" ) );
    assertThat( thinResultSet.getBoolean( "bool" ), equalTo( false ) );
  }

  @Test public void testGetDate() throws Exception {
    ValueMetaDate valueMetaDate = new ValueMetaDate( "foo" );
    long time = System.currentTimeMillis();
    thinResultSet.currentRow = new Object[] { new Date( time ) };
    rowMeta.addValueMeta( valueMetaDate );
    assertThat( thinResultSet.getDate( "foo" ), equalTo( new java.sql.Date( time ) ) );
    assertThat( thinResultSet.getTimestamp( "foo" ), equalTo( new java.sql.Timestamp( time ) ) );
    assertThat( thinResultSet.getTime( "foo" ), equalTo( new java.sql.Time( time ) ) );
  }

  @Test public void testGetDouble() throws Exception {
    thinResultSet.currentRow = new Object[] { 1.1, "foo" };
    rowMeta.addValueMeta( new ValueMetaNumber( "number" ) );
    rowMeta.addValueMeta( new ValueMetaString( "string" ) );
    assertThat( thinResultSet.getDouble( "number" ), equalTo( 1.1 ) );
    assertThat( thinResultSet.getFloat( "number" ), equalTo( 1.1f ) );
  }

  @Test public void testGetBigDecimal() throws Exception {
    thinResultSet.currentRow = new Object[] { 1.1, "foo" };
    rowMeta.addValueMeta( new ValueMetaNumber( "number" ) );
    rowMeta.addValueMeta( new ValueMetaString( "string" ) );
    assertThat( thinResultSet.getBigDecimal( "number" ).doubleValue(), equalTo( 1.1 ) );
  }

  @Test public void testGetByte() throws Exception {
    byte b = 'a';
    thinResultSet.currentRow = new Object[] { new Byte( b ).longValue(), "foo" };
    rowMeta.addValueMeta( new ValueMetaInteger( "byte" ) );
    rowMeta.addValueMeta( new ValueMetaString( "string" ) );
    assertThat( thinResultSet.getByte( "byte" ), equalTo( b ) );
  }

  @Test public void testGetBytes() throws Exception {
    thinResultSet.currentRow = new Object[] { "b", "foo" };
    rowMeta.addValueMeta( new ValueMetaString( "bytes" ) );
    rowMeta.addValueMeta( new ValueMetaString( "string" ) );
    assertThat( thinResultSet.getBytes( "bytes" ), equalTo( new byte[] { 'b' } ) );

  }

  @Test public void testGetFloat() throws Exception {
    thinResultSet.currentRow = new Object[] { 1.1, "foo" };
    rowMeta.addValueMeta( new ValueMetaNumber( "col" ) );
    rowMeta.addValueMeta( new ValueMetaString( "string" ) );
    assertThat( thinResultSet.getFloat( "col" ), equalTo( 1.1f ) );
  }

  @Test public void testGetInt() throws Exception {
    thinResultSet.currentRow = new Object[] { 1l, "foo" };
    rowMeta.addValueMeta( new ValueMetaInteger( "col" ) );
    rowMeta.addValueMeta( new ValueMetaString( "string" ) );
    assertThat( thinResultSet.getInt( "col" ), equalTo( 1 ) );

  }

  @Test public void testGetLong() throws Exception {
    thinResultSet.currentRow = new Object[] { 1l, "foo" };
    rowMeta.addValueMeta( new ValueMetaInteger( "col" ) );
    rowMeta.addValueMeta( new ValueMetaString( "string" ) );
    assertThat( thinResultSet.getLong( "col" ), equalTo( 1l ) );
    assertThat( thinResultSet.getInt( "col" ), equalTo( 1 ) );
  }

  @Test public void testGetShort() throws Exception {
    thinResultSet.currentRow = new Object[] { 1l, "foo" };
    rowMeta.addValueMeta( new ValueMetaInteger( "col" ) );
    rowMeta.addValueMeta( new ValueMetaString( "string" ) );
    short s = 1;
    assertThat( thinResultSet.getShort( "col" ), equalTo( s ) );

  }

  @Test public void testGetString() throws Exception {
    thinResultSet.currentRow = new Object[] { "a string", "foo" };
    rowMeta.addValueMeta( new ValueMetaString( "col" ) );
    rowMeta.addValueMeta( new ValueMetaString( "string" ) );
    assertThat( thinResultSet.getString( "col" ), equalTo( "a string" ) );
    assertThat( thinResultSet.getObject( "col", String.class ), equalTo( "a string" ) );
  }

  @Test
  public void testGetColumnNotPresentDouble() throws Exception {
    thinResultSet.currentRow = new Object[] { 1.1, "foo" };
    rowMeta.addValueMeta( new ValueMetaNumber( "number" ) );
    rowMeta.addValueMeta( new ValueMetaString( "string" ) );
    try {
      thinResultSet.getDouble( "nonesuch" );
    } catch ( SQLException e ) {
      assertThat( e.getMessage(), equalTo( INVALID_COLUMN_REFERENCE + "nonesuch" ) );
      return;
    }
    fail();
  }

  @Test
  public void testGetColumnNotPresentString() throws Exception {
    thinResultSet.currentRow = new Object[] { 1.1, "foo" };
    rowMeta.addValueMeta( new ValueMetaNumber( "number" ) );
    rowMeta.addValueMeta( new ValueMetaString( "string" ) );
    try {
      thinResultSet.getString( "nonesuch" );
    } catch ( SQLException e ) {
      assertThat( e.getMessage(), equalTo( INVALID_COLUMN_REFERENCE + "nonesuch" ) );
      return;
    }
    fail();
  }

  @Test
  public void testGetColumnIndexNotPresentString() throws Exception {
    thinResultSet.currentRow = new Object[] { 1.1, "foo" };
    rowMeta.addValueMeta( new ValueMetaNumber( "number" ) );
    rowMeta.addValueMeta( new ValueMetaString( "string" ) );
    try {
      thinResultSet.getString( 4 );
    } catch ( SQLException e ) {
      assertThat( e.getMessage(), equalTo( INVALID_COLUMN_REFERENCE + 4 ) );
      return;
    }
    fail();
  }

  @Test
  public void testGetColumnNotPresentObject() throws Exception {
    thinResultSet.currentRow = new Object[] { 1.1, "foo" };
    rowMeta.addValueMeta( new ValueMetaNumber( "number" ) );
    rowMeta.addValueMeta( new ValueMetaString( "string" ) );
    try {
      thinResultSet.getObject( "nonesuch" );
    } catch ( SQLException e ) {
      assertThat( e.getMessage(), equalTo( INVALID_COLUMN_REFERENCE + "nonesuch" ) );
      return;
    }
    fail();
  }

  @Override protected ThinResultSet getTestObject() {
    return thinResultSet;
  }
}
