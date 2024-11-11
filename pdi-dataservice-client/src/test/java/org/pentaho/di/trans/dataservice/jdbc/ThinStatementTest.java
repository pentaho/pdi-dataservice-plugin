/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.di.trans.dataservice.jdbc;

import com.google.common.collect.ImmutableMap;
import io.reactivex.subjects.PublishSubject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService.IStreamingParams;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService.StreamingMode;

import java.io.DataInputStream;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
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

    when( connection.getClientService() ).thenReturn( clientService );
    when( resultFactory.loadResultSet( Mockito.<DataInputStream>any(), Mockito.<IDataServiceClientService>any() ) )
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
    assertThat( statement.getResultSetType(), is( resultSet.getType() ) );
    assertThat( statement.getUpdateCount(), equalTo( 0 ) );

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

  @Test
  public void testExecutePushQuery() throws Exception {
    IStreamingParams params = new IStreamingParams () {
      @Override
      public StreamingMode getWindowMode() {
        return IDataServiceClientService.StreamingMode.ROW_BASED;
      }
      @Override
      public long getWindowSize() {
        return 3;
      }
      @Override
      public long getWindowEvery() {
        return 2;
      }
      public long getWindowLimit() {
        return 1000;
      }
    };
    when( connection.isLocal() ).thenReturn( true );
    PublishSubject<List<RowMetaAndData>> consumer = PublishSubject.create();
    statement.executePushQuery( SQL, params, consumer );
    verify( clientService ).query( eq( SQL ), eq( params ), any(), eq( consumer ) );
  }

  @Test( expected = UnsupportedOperationException.class )
  public void testExecutePushQueryNonLocal() throws Exception {
    when( connection.isLocal() ).thenReturn( false );
    PublishSubject<List<RowMetaAndData>> consumer = PublishSubject.create();
    statement.executePushQuery( SQL, mock( IStreamingParams.class ), consumer );
  }

  @Test
  public void unwrapTest() throws Exception {
    assertTrue( statement.isWrapperFor( ThinStatement.class ) );
    assertEquals( ThinStatement.class, statement.unwrap( Statement.class ).getClass() );
  }
  @Test
  public void unwrapTestDS() throws Exception {
    assertTrue( statement.isWrapperFor( IDataServiceClientService.class ) );
    IDataServiceClientService ds = statement.unwrap( IDataServiceClientService.class );
    assertNotNull( ds );
  }

  @Test( expected = SQLException.class )
  public void unwrapFailTest() throws Exception {
    assertFalse( statement.isWrapperFor( String.class ) );
    statement.unwrap( String.class );
  }

  @Override protected ThinStatement getTestObject() {
    return statement;
  }

}
