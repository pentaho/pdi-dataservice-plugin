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
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.exception.KettleEOFException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.value.ValueMetaBoolean;
import org.pentaho.di.core.row.value.ValueMetaDate;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaNumber;
import org.pentaho.di.core.row.value.ValueMetaString;

import java.io.DataInputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;

import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith( MockitoJUnitRunner.class )
public class ThinResultSetTest extends JDBCTestBase<ThinResultSet> {

  public static final String INVALID_COLUMN_REFERENCE = "Invalid column reference: ";
  ThinResultHeader resultHeader;
  ThinResultSet thinResultSet;
  @Mock DataInputStream dataInputStream;
  @Spy RowMeta rowMeta;
  Object[] currentRow = new Object[0];

  public ThinResultSetTest() {
    super( ThinResultSet.class );
  }

  @Before
  public void setUp() throws Exception {
    resultHeader = new ThinResultHeader( "resultSetTest", "serviceTrans", "serviceId", "sqlTrans", "sqlId", rowMeta );
    thinResultSet = new ThinResultSet( resultHeader, dataInputStream );
    doAnswer( new Answer() {
      @Override public Object[] answer( InvocationOnMock invocation ) throws Throwable {
        return currentRow;
      }
    } ).when( rowMeta ).readData( dataInputStream );
    assertThat( thinResultSet.getHeader(), equalTo( resultHeader ) );
  }

  @Test
  public void testGetStatement() throws Exception {
    ThinStatement thinStatement = mock( ThinStatement.class );
    thinResultSet.setStatement( thinStatement );
    assertThat( thinResultSet.getStatement(), sameInstance( (Statement) thinStatement ) );
  }

  @Test
  public void testMetaData() throws Exception {
    rowMeta.addValueMeta( new ValueMetaString( "X" ) );
    ResultSetMetaData metaData = thinResultSet.getMetaData();

    assertThat( metaData.getTableName( 1 ), equalTo( "resultSetTest" ) );
    assertThat( metaData.getColumnName( 1 ), equalTo( "X" ) );
  }

  @Test
  public void testEndOfResultSet() throws Exception {
    assertThat( thinResultSet.isBeforeFirst(), is( true ) );
    for ( int i = 0; i < 3; i++ ) {
      assertThat( thinResultSet.next(), is( true ) );
    }
    assertThat( thinResultSet.getRow(), is( 3 ) );
    assertThat( thinResultSet.isBeforeFirst(), is( false ) );

    assertThat( thinResultSet.absolute( 3 ), is( true ) );
    try {
      assertThat( thinResultSet.previous(), not( anything() ) );
    } catch ( SQLException e ) {
      assertThat( e, instanceOf( SQLFeatureNotSupportedException.class ) );
    }

    doThrow( new KettleEOFException() ).when( rowMeta ).readData( dataInputStream );
    assertThat( thinResultSet.next(), is( false ) );
    assertThat( thinResultSet.isAfterLast(), is( true ) );
    assertThat( thinResultSet.isClosed(), is( true ) );
    verify( dataInputStream ).close();
  }

  @Test
  public void testEmptyResultSet() throws Exception {
    doThrow( new KettleEOFException() ).when( rowMeta ).readData( dataInputStream );

    assertThat( thinResultSet.next(), is( false ) );
    assertThat( thinResultSet.isBeforeFirst(), is( true ) );
    assertThat( thinResultSet.isClosed(), is( true ) );

    verify( rowMeta ).readData( dataInputStream );
    assertThat( thinResultSet.next(), is( false ) );
    verifyNoMoreInteractions( rowMeta );
  }

  @Test
  public void testClose() throws Exception {
    assertThat( thinResultSet.isClosed(), is( false ) );

    thinResultSet.close();

    assertThat( thinResultSet.isClosed(), is( true ) );
    assertThat( thinResultSet.isBeforeFirst(), is( true ) );

    thinResultSet.close();

    verify( dataInputStream ).close();
  }

  @Test public void testGetBoolean() throws Exception {
    currentRow = new Object[] { false, "foo" };
    rowMeta.addValueMeta( new ValueMetaBoolean( "bool" ) );
    rowMeta.addValueMeta( new ValueMetaString( "string" ) );
    thinResultSet.next();
    assertThat( thinResultSet.getBoolean( "bool" ), equalTo( false ) );
  }

  @Test public void testGetNull() throws Exception {
    currentRow = new Object[] { null };
    rowMeta.addValueMeta( new ValueMetaString( "foo" ) );
    thinResultSet.next();
    assertThat( thinResultSet.getString( "foo" ), nullValue() );
    assertThat( thinResultSet.wasNull(), is( true ) );
  }

  @Test public void testGetDate() throws Exception {
    long time = System.currentTimeMillis();
    currentRow = new Object[] { new Date( time ) };
    rowMeta.addValueMeta( new ValueMetaDate( "foo" ) );
    thinResultSet.next();
    assertThat( thinResultSet.getDate( "foo" ), equalTo( new java.sql.Date( time ) ) );
    assertThat( thinResultSet.getTimestamp( "foo" ), equalTo( new java.sql.Timestamp( time ) ) );
    assertThat( thinResultSet.getTime( "foo" ), equalTo( new java.sql.Time( time ) ) );
  }

  @Test public void testGetDouble() throws Exception {
    currentRow = new Object[] { 1.1, "foo" };
    rowMeta.addValueMeta( new ValueMetaNumber( "number" ) );
    rowMeta.addValueMeta( new ValueMetaString( "string" ) );
    thinResultSet.next();
    assertThat( thinResultSet.getDouble( "number" ), equalTo( 1.1 ) );
    assertThat( thinResultSet.getFloat( "number" ), equalTo( 1.1f ) );
  }

  @Test public void testGetBigDecimal() throws Exception {
    currentRow = new Object[] { 1.1, "foo" };
    rowMeta.addValueMeta( new ValueMetaNumber( "number" ) );
    rowMeta.addValueMeta( new ValueMetaString( "string" ) );
    thinResultSet.next();
    assertThat( thinResultSet.getBigDecimal( "number" ).doubleValue(), equalTo( 1.1 ) );
  }

  @Test public void testGetByte() throws Exception {
    byte b = 'a';
    currentRow = new Object[] { new Byte( b ).longValue(), "foo" };
    rowMeta.addValueMeta( new ValueMetaInteger( "byte" ) );
    rowMeta.addValueMeta( new ValueMetaString( "string" ) );
    thinResultSet.next();
    assertThat( thinResultSet.getByte( "byte" ), equalTo( b ) );
  }

  @Test public void testGetBytes() throws Exception {
    currentRow = new Object[] { "b", "foo" };
    rowMeta.addValueMeta( new ValueMetaString( "bytes" ) );
    rowMeta.addValueMeta( new ValueMetaString( "string" ) );
    thinResultSet.next();
    assertThat( thinResultSet.getBytes( "bytes" ), equalTo( new byte[] { 'b' } ) );

  }

  @Test public void testGetFloat() throws Exception {
    currentRow = new Object[] { 1.1, "foo" };
    rowMeta.addValueMeta( new ValueMetaNumber( "col" ) );
    rowMeta.addValueMeta( new ValueMetaString( "string" ) );
    thinResultSet.next();
    assertThat( thinResultSet.getFloat( "col" ), equalTo( 1.1f ) );
  }

  @Test public void testGetInt() throws Exception {
    currentRow = new Object[] { 1l, "foo" };
    rowMeta.addValueMeta( new ValueMetaInteger( "col" ) );
    rowMeta.addValueMeta( new ValueMetaString( "string" ) );
    thinResultSet.next();
    assertThat( thinResultSet.getInt( "col" ), equalTo( 1 ) );

  }

  @Test public void testGetLong() throws Exception {
    currentRow = new Object[] { 1l, "foo" };
    rowMeta.addValueMeta( new ValueMetaInteger( "col" ) );
    rowMeta.addValueMeta( new ValueMetaString( "string" ) );
    thinResultSet.next();
    assertThat( thinResultSet.getLong( "col" ), equalTo( 1l ) );
    assertThat( thinResultSet.getInt( "col" ), equalTo( 1 ) );
  }

  @Test public void testGetShort() throws Exception {
    currentRow = new Object[] { 1l, "foo" };
    rowMeta.addValueMeta( new ValueMetaInteger( "col" ) );
    rowMeta.addValueMeta( new ValueMetaString( "string" ) );
    short s = 1;
    thinResultSet.next();
    assertThat( thinResultSet.getShort( "col" ), equalTo( s ) );

  }

  @Test public void testGetString() throws Exception {
    currentRow = new Object[] { "a string", "foo" };
    rowMeta.addValueMeta( new ValueMetaString( "col" ) );
    rowMeta.addValueMeta( new ValueMetaString( "string" ) );
    thinResultSet.next();
    assertThat( thinResultSet.getString( "col" ), equalTo( "a string" ) );
    assertThat( thinResultSet.getObject( "col", String.class ), equalTo( "a string" ) );
  }

  @Test
  public void testGetColumnNotPresentDouble() throws Exception {
    currentRow = new Object[] { 1.1, "foo" };
    rowMeta.addValueMeta( new ValueMetaNumber( "number" ) );
    rowMeta.addValueMeta( new ValueMetaString( "string" ) );
    thinResultSet.next();
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
    currentRow = new Object[] { 1.1, "foo" };
    rowMeta.addValueMeta( new ValueMetaNumber( "number" ) );
    rowMeta.addValueMeta( new ValueMetaString( "string" ) );
    thinResultSet.next();
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
    currentRow = new Object[] { 1.1, "foo" };
    rowMeta.addValueMeta( new ValueMetaNumber( "number" ) );
    rowMeta.addValueMeta( new ValueMetaString( "string" ) );
    thinResultSet.next();
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
    currentRow = new Object[] { 1.1, "foo" };
    rowMeta.addValueMeta( new ValueMetaNumber( "number" ) );
    rowMeta.addValueMeta( new ValueMetaString( "string" ) );
    thinResultSet.next();
    try {
      thinResultSet.getObject( "nonesuch" );
    } catch ( SQLException e ) {
      assertThat( e.getMessage(), equalTo( INVALID_COLUMN_REFERENCE + "nonesuch" ) );
      return;
    }
    fail();
  }

  @Test
  public void testFetchDirection() throws Exception {
    assertThat( thinResultSet.getType(), equalTo( ResultSet.TYPE_FORWARD_ONLY ) );
    assertThat( thinResultSet.getFetchDirection(), equalTo( ResultSet.FETCH_FORWARD ) );
    thinResultSet.setFetchDirection( ResultSet.FETCH_FORWARD );
    try {
      thinResultSet.setFetchDirection( ResultSet.FETCH_REVERSE );
      fail( "Expected setFetchDirection(REVERSE) to fail" );
    } catch ( Exception e ) {
      assertThat( e, instanceOf( SQLFeatureNotSupportedException.class ) );
    }
  }

  @Test
  public void testProperties() throws Exception {
    thinResultSet.setFetchSize( ThreadLocalRandom.current().nextInt() );
    assertThat( thinResultSet.getFetchSize(), is( 0 ) );

    assertThat( thinResultSet.getConcurrency(), is( ResultSet.CONCUR_READ_ONLY ) );
    assertThat( thinResultSet.getHoldability(), anything() );
    assertThat( thinResultSet.getCursorName(), anything() );
  }

  @Override protected ThinResultSet getTestObject() {
    return thinResultSet;
  }
}
