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

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;

import java.io.DataInputStream;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author nhudak
 */
@RunWith( MockitoJUnitRunner.class )
public class ThinStatementTest extends JDBCTestBase<ThinStatement> {

  public static final String SQL = "SELECT * FROM dataService";
  public static final String SERVICE_OBJECT_ID = "12345";

  @Mock ThinConnection connection;
  @Mock ThinResultSet resultSet;
  @Mock IDataServiceClientService clientService;
  @Mock ThinResultFactory resultFactory;
  @Mock ThinResultHeader header;
  @Mock ImmutableMap<String, String> mockParameters;

  ThinStatement statement;

  public ThinStatementTest() {
    super( ThinStatement.class );
  }

  @Before
  public void setUp() throws Exception {
    when( connection.getParameters() ).thenReturn( mockParameters );
    statement = new ThinStatement( connection, resultFactory );
    assertThat( statement.getConnection(), sameInstance( (Connection) connection ) );

    when( clientService.query( anyString(), anyInt() ) ).then( new Answer<DataInputStream>() {
      @Override public DataInputStream answer( InvocationOnMock invocation ) throws Throwable {
        return mock( DataInputStream.class );
      }
    } );

    when( clientService.query( anyString(), any(), anyLong(), anyLong(), anyLong() ) )
            .then( new Answer<DataInputStream>() {
      @Override public DataInputStream answer( InvocationOnMock invocation ) throws Throwable {
        return mock( DataInputStream.class );
      }
    } );

    when( resultSet.getHeader() ).thenReturn( header );
    when( header.getServiceObjectId() ).thenReturn( SERVICE_OBJECT_ID );
    when( connection.getClientService() ).thenReturn( clientService );
    when( resultFactory.loadResultSet( any( DataInputStream.class ), any( IDataServiceClientService.class ) ) )
        .thenReturn( resultSet );
  }

  @Test
  public void testExecute() throws Exception {
    int n = 0;
    for ( Method method : Statement.class.getMethods() ) {
      if ( "execute".equals( method.getName() ) ) {
        assertThat( invoke( statement, method ), equalTo( (Object) Boolean.TRUE ) );
        verify( clientService, times( ++n ) ).query( anyString(), anyInt(), anyMap() );
      }
    }
  }

  @Test
  public void testExecuteQuery() throws Exception {
    DataInputStream inputStream = MockDataInput.dual().toDataInputStream();
    when( clientService.query( SQL, 32, mockParameters ) ).thenReturn( inputStream );

    statement.setMaxRows( 32 );
    assertThat( statement.getMaxRows(), equalTo( 32 ) );

    assertThat( statement.executeQuery( SQL ), sameInstance( (ResultSet) resultSet ) );
    assertThat( statement.getResultSet(), sameInstance( (ResultSet) resultSet ) );
    verify( resultSet ).setStatement( statement );

    when( resultSet.getConcurrency() ).thenReturn( ResultSet.CONCUR_READ_ONLY );
    when( resultSet.getHoldability() ).thenReturn( ResultSet.HOLD_CURSORS_OVER_COMMIT );
    when( resultSet.isClosed() ).thenReturn( false );
    assertThat( statement.getResultSetConcurrency(), is( ResultSet.CONCUR_READ_ONLY ) );
    assertThat( statement.getResultSetHoldability(), is( ResultSet.HOLD_CURSORS_OVER_COMMIT ) );
    assertThat( statement.isClosed(), is( false ) );

    statement.close();
    verify( resultSet ).close();
  }

  @Test
  public void testExecuteQueryWindow() throws Exception {
    DataInputStream inputStream = MockDataInput.dual().toDataInputStream();
    when( clientService.query( SQL, IDataServiceClientService.StreamingMode.ROW_BASED,
            1, 2, 3, mockParameters ) ).thenReturn( inputStream );

    statement.setMaxRows( 32 );
    assertThat( statement.getMaxRows(), equalTo( 32 ) );

    assertThat( statement.executeQuery( SQL, IDataServiceClientService.StreamingMode.ROW_BASED,
            1, 2, 3 ), sameInstance( (ResultSet) resultSet ) );
    assertThat( statement.getResultSet(), sameInstance( (ResultSet) resultSet ) );
    verify( resultSet ).setStatement( statement );

    when( resultSet.getConcurrency() ).thenReturn( ResultSet.CONCUR_READ_ONLY );
    when( resultSet.getHoldability() ).thenReturn( ResultSet.HOLD_CURSORS_OVER_COMMIT );
    when( resultSet.isClosed() ).thenReturn( false );
    assertThat( statement.getResultSetConcurrency(), is( ResultSet.CONCUR_READ_ONLY ) );
    assertThat( statement.getResultSetHoldability(), is( ResultSet.HOLD_CURSORS_OVER_COMMIT ) );
    assertThat( statement.isClosed(), is( false ) );

    statement.close();
    verify( resultSet ).close();
  }

  @Test
  public void testGetMoreResults() throws Exception {
    try {
      assertThat( statement.getMoreResults(), not( anything() ) );
    } catch ( SQLException e ) {
      assertThat( e, anything() );
    }

    assertThat( statement.execute( SQL ), is( true ) );
    when( resultSet.isLast() ).thenReturn( false, true );

    try {
      assertThat( statement.getMoreResults( Statement.CLOSE_ALL_RESULTS ), not( anything() ) );
    } catch ( SQLException e ) {
      assertThat( e, instanceOf( SQLFeatureNotSupportedException.class ) );
    }
    assertThat( statement.getMoreResults( Statement.CLOSE_CURRENT_RESULT ), is( true ) );
    assertThat( statement.getMoreResults(), is( false ) );
    verify( resultSet ).close();
  }

  @Test
  public void testUnusedProperties() throws Exception {
    statement.setEscapeProcessing( false );

    try {
      statement.setFetchDirection( ResultSet.FETCH_REVERSE );
      fail();
    } catch ( SQLException e ) {
      assertThat( e, instanceOf( SQLFeatureNotSupportedException.class ) );
      statement.setFetchDirection( ResultSet.FETCH_FORWARD );
    }
    assertThat( statement.getFetchDirection(), equalTo( ResultSet.FETCH_FORWARD ) );

    statement.setFetchSize( 256 );
    assertThat( statement.getFetchSize(), equalTo( 1 ) );

    statement.setMaxFieldSize( 10 );
    assertThat( statement.getMaxFieldSize(), equalTo( 0 ) );

    statement.setPoolable( true );
    assertThat( statement.isPoolable(), equalTo( false ) );

    statement.setQueryTimeout( 1000 );
    assertThat( statement.getQueryTimeout(), equalTo( 0 ) );

    statement.closeOnCompletion();
    assertThat( statement.isCloseOnCompletion(), equalTo( false ) );
  }

  @Test
  public void testMaxRows() throws Exception {
    statement.setMaxRows( 42 );
    assertThat( statement.getMaxRows(), equalTo( 42 ) );
  }

  @Override protected ThinStatement getTestObject() {
    return statement;
  }
}
