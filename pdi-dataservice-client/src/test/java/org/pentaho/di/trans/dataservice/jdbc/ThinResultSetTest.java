/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.exception.KettleEOFException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;

import java.io.DataInputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.concurrent.ThreadLocalRandom;

import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.containsStringIgnoringCase;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class ThinResultSetTest extends BaseResultSetTest {

  ThinResultSet thinResultSet;
  ThinResultHeader resultHeader;
  @Mock DataInputStream dataInputStream;
  @Mock IDataServiceClientService client;
  Object[] nextRow = new Object[0];

  public ThinResultSetTest() {
    super( ThinResultSet.class );
  }

  @Before
  public void setUp() throws Exception {
    resultHeader = new ThinResultHeader( "resultSetTest", "serviceTrans", "serviceId", "sqlTrans", "sqlId", rowMeta );
    thinResultSet = new ThinResultSet( resultHeader, dataInputStream, client );
    doAnswer( new Answer() {
      @Override public Object[] answer( InvocationOnMock invocation ) throws Throwable {
        return nextRow;
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
  public void testResultSetIteration() throws Exception {
    verifyState( "beforeFirst" );

    assertThat( thinResultSet.next(), is( true ) );
    assertThat( thinResultSet.getRow(), is( 1 ) );
    verifyState( "first" );

    assertThat( thinResultSet.next(), is( true ) );
    assertThat( thinResultSet.getRow(), is( 2 ) );
    verifyState();

    assertThat( thinResultSet.relative( 0 ), is( true ) );
    assertThat( thinResultSet.getRow(), is( 2 ) );
    verifyState();

    doThrow( new KettleFileException() ).when( rowMeta ).readData( dataInputStream );

    assertThat( thinResultSet.next(), is( false ) );
    assertThat( thinResultSet.getRow(), is( 0 ) );
    verifyState( "afterLast" );

    assertThat( thinResultSet.next(), is( false ) );

    try {
      assertThat( thinResultSet.previous(), not( anything() ) );
    } catch ( SQLException e ) {
      assertThat( e, instanceOf( SQLFeatureNotSupportedException.class ) );
    }
    verify( dataInputStream ).close();
  }

  @Test
  public void testEmptyResultSet() throws Exception {
    doThrow( new KettleEOFException() ).when( rowMeta ).readData( dataInputStream );

    verifyState( "beforeFirst" );
    assertThat( thinResultSet.next(), is( false ) );
    verifyState( "afterLast" );

    verify( rowMeta ).readData( dataInputStream );
    assertThat( thinResultSet.next(), is( false ) );
    verifyNoMoreInteractions( rowMeta );
  }

  @Test
  public void testNullDataInputStream() throws Exception {
    ThinResultSet thinResultSetSpy = spy( thinResultSet );
    doThrow( new KettleFileException() ).when( rowMeta ).readData( any( java.io.DataInputStream.class) );

    doReturn( true ).when( thinResultSetSpy ).isClosed();
    doReturn( true ).when( thinResultSetSpy ).isAfterLast();
    doReturn( 5 ).when( thinResultSetSpy ).size();
    doReturn( 0 ).when( thinResultSetSpy ).getRow();

    thinResultSetSpy.retrieveRow( 1 );
    verify( dataInputStream, never() ).close();
  }

  @Test
  public void testClose() throws Exception {
    verifyState( "beforeFirst" );
    assertThat( thinResultSet.isClosed(), is( false ) );

    DataInputStream errorInputStream = MockDataInput.errors().toDataInputStream();
    DataInputStream stopInputStream = MockDataInput.stop().toDataInputStream();

    when( client.query( "[ errors serviceId ]", 0 ) ).thenReturn( errorInputStream );
    when( client.query( "[ stop serviceId ]", 0 ) ).thenReturn( stopInputStream );

    thinResultSet.close();

    assertThat( thinResultSet.isClosed(), is( true ) );

    thinResultSet.close();

    verify( dataInputStream ).close();

    for ( Method method : STATES.values() ) {
      try {
        assertThat( invoke( thinResultSet, method ), not( anything() ) );
      } catch ( InvocationTargetException e ) {
        assertThat( e.getCause().getMessage(), containsStringIgnoringCase( "closed" ) );
      }
    }
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

  public void setNextRow( Object[] nextRow ) {
    this.nextRow = nextRow;
  }
}
