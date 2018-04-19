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

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import org.hamcrest.CustomMatcher;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.trans.dataservice.client.ConnectionAbortingSupport;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;

import java.lang.reflect.Method;
import java.net.URI;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.doThrow;

/**
 * Created by bmorrise on 9/28/15.
 */
@RunWith( MockitoJUnitRunner.class )
public class ThinConnectionTest extends JDBCTestBase<ThinConnection> {

  private ThinConnection connection;
  private Properties properties;

  @Mock
  private RemoteClient clientService;
  @Mock
  private ThinStatement stm;

  private static String host = "localhost";
  private static int port = 8080;
  private static String debugTrans = "debugTrans";
  private static String maxRows = "100";
  private static String windowRows = "1";
  private static String windowTime = "2";
  private static String windowRate = "3";
  private Matcher<String> noBangMatcher;

  private String url;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  public ThinConnectionTest() {
    super( ThinConnection.class );
  }

  @Before
  public void setUp() throws Exception {
    url = "jdbc:pdi://localhost:8080/pentaho/kettle";

    properties = new Properties();
    properties.setProperty( "user", "username" );
    properties.setProperty( "password", "password" );
    properties.setProperty( "maxrows", maxRows );
    properties.setProperty( "windowrows", windowRows );
    properties.setProperty( "windowtime", windowTime );
    properties.setProperty( "windowrate", windowRate );

    connection = new ThinConnection( url, URI.create( "http://localhost:8080/pentaho/kettle" ) );
    connection.setClientService( clientService );

    //when( connectionManager.createHttpClient() ).thenReturn( httpClient );

    // This sets up a rule about exception messages not containing exclamation-points.
    // By default, BaseMessages.getString will return the key passed in surrounded by exclamation-points
    // if the message cannot be found in the bundle. This makes sure that all messages for the unsupported
    // exceptions are able to be found.
    noBangMatcher = new CustomMatcher<String>( "No Bangs" ) {
      public boolean matches( Object object ) {
        return ( object instanceof String ) && !( ( String )object).contains( "!" );
      }
    };

  }

  @Test
  public void testBuilder() throws Exception {
    properties.setProperty( "debugtrans", debugTrans );
    url += "?PARAMETER_TRANS_PARAM=yes";
    ThinConnection thinConnection = new ThinConnection();
    ThinConnection.Builder builder = thinConnection.createBuilder();
    connection = builder.parseUrl( url ).readProperties( properties ).build();

    assertEquals( url, connection.getUrl() );
    assertEquals( host, connection.getHostname() );
    assertEquals( port, connection.getPort() );

    assertEquals( "username", connection.getUsername() );
    assertEquals( maxRows, connection.getMaxRows() );
    assertEquals( windowRows, connection.getWindowRows() );
    assertEquals( windowTime, connection.getWindowTime() );
    assertEquals( windowRate, connection.getWindowRate() );

    assertThat( connection.getDebugTransFilename(), is( debugTrans ) );
    assertEquals( false, connection.isLocal() );

    assertThat( connection.getParameters(), equalTo( ImmutableMap.of( "PARAMETER_TRANS_PARAM", "yes" ) ) );

    assertThat( connection.constructUrl( "/service?argument=value" ),
      equalTo( "http://localhost:8080/pentaho/kettle/service?argument=value" ) );
  }

  @Test
  public void testCarteConnection() throws Exception {
    url = "jdbc:pdi://localhost/kettle";
    ThinConnection thinConnection = new ThinConnection();
    ThinConnection.Builder builder = thinConnection.createBuilder();
    connection = builder.parseUrl( url ).readProperties( properties ).build();

    assertThat( connection, allOf(
      hasProperty( "hostname", is( host ) ),
      hasProperty( "port", is( 80 ) ),
      hasProperty( "clientService", instanceOf( RemoteClient.class ) )
    ) );
    assertThat( connection.constructUrl( "/service?argument=value" ),
      equalTo( "http://localhost:80/kettle/service?argument=value" ) );
  }

  @Test
  public void testLegacyBuilder() throws Exception {
    final String proxyHostName = "proxyhostname", proxyPort = "9081", nonProxyHosts = "nonproxyhost";
    ImmutableMap<String, String> args = ImmutableMap.<String, String>builder().
      put( "webappname", "pentaho" ).
      put( "proxyhostname", proxyHostName ).
      put( "proxyport", proxyPort ).
      put( "nonproxyhosts", nonProxyHosts ).
      put( "debugtrans", debugTrans ).
      put( "secure", "true" ).
      put( "local", "false" ).
      put( "maxRows", maxRows ).
      put( "windowrows", windowRows ).
      put( "windowtime", windowTime ).
      put( "windowrate", windowRate ).
      put( "PARAMETER_HELLO_WORLD", URLEncoder.encode( "test value", Charsets.UTF_8.name() ) ).
      build();
    url = "jdbc:pdi://localhost:8080/kettle?" + Joiner.on( "&" ).withKeyValueSeparator( "=" ).join( args );

    ThinConnection.Builder builder = new ThinConnection().createBuilder();
    ThinConnection thinConnection = builder.parseUrl( url ).readProperties( properties ).build();

    assertThat( thinConnection.getClientService(), instanceOf( RemoteClient.class ) );

    assertEquals( url, thinConnection.getUrl() );
    assertEquals( host, thinConnection.getHostname() );
    assertEquals( port, thinConnection.getPort() );
    assertEquals( "username", thinConnection.getUsername() );
    assertEquals( maxRows, thinConnection.getMaxRows() );
    assertEquals( windowRows, thinConnection.getWindowRows() );
    assertEquals( windowTime, thinConnection.getWindowTime() );
    assertEquals( windowRate, thinConnection.getWindowRate() );
    assertEquals( proxyHostName, thinConnection.getProxyHostname() );
    assertEquals( proxyPort, thinConnection.getProxyPort() );
    assertEquals( nonProxyHosts, thinConnection.getNonProxyHosts() );
    assertEquals( debugTrans, thinConnection.getDebugTransFilename() );
    assertEquals( true, thinConnection.isSecure() );
    assertEquals( false, thinConnection.isLocal() );

    assertThat( thinConnection.getParameters(), equalTo( ImmutableMap.of( "PARAMETER_HELLO_WORLD", "test value" ) ) );

    assertThat( thinConnection.constructUrl( "/service?argument=value" ),
      equalTo( "https://localhost:8080/pentaho/kettle/service?argument=value" ) );
  }

  @Test
  public void testLocalConnection() throws Exception {
    ThinConnection.localClient = mock( IDataServiceClientService.class );

    connection = new ThinConnection().createBuilder().parseUrl( "pdi:jdbc://localhost:-1?local=true" ).build();
    assertThat( connection.getClientService(), sameInstance( ThinConnection.localClient ) );
  }

  @Test
  public void testValid() throws Exception {
    when( clientService.query( eq( "SELECT *" ), anyInt(), anyMap() ) )
      .thenThrow( new SQLException( "Expected exception" ) )
      .thenReturn( MockDataInput.dual().toDataInputStream() );

    assertThat( connection.isValid( 0 ), is( false ) );
    assertThat( connection.getWarnings().getMessage(), containsString( "Expected exception" ) );

    connection.clearWarnings();
    if ( !connection.isValid( 0 ) ) {
      throw new AssertionError( connection.getWarnings() );
    }
    assertThat( connection.getWarnings(), nullValue() );
  }

  @Test
  public void testPrepareStatement() throws Exception {
    for ( Method method : Connection.class.getMethods() ) {
      if ( "prepareStatement".equals( method.getName() ) ) {
        assertThat( invoke( connection, method ), instanceOf( ThinPreparedStatement.class ) );
      }
    }
  }

  @Test
  public void testCreateStatement() throws Exception {
    for ( Method method : Connection.class.getMethods() ) {
      if ( "createStatement".equals( method.getName() ) ) {
        assertThat( invoke( connection, method ), instanceOf( ThinStatement.class ) );
      }
    }
  }

  @Test
  public void testUnusedProperties() throws Exception {
    connection.setSchema( "schema " );
    assertThat( connection.getSchema(), nullValue() );

    connection.setCatalog( "catalog" );
    assertThat( connection.getCatalog(), nullValue() );

    connection.setAutoCommit( false );
    assertThat( connection.getAutoCommit(), is( true ) );

    connection.setReadOnly( false );
    assertThat( connection.isReadOnly(), is( true ) );

    connection.setClientInfo( properties );
    assertThat( connection.getClientInfo(), anEmptyMap() );

    assertThat( connection.getTransactionIsolation(), is( Connection.TRANSACTION_NONE ) );

    connection.setHoldability( RowsResultSet.CLOSE_CURSORS_AT_COMMIT );
    assertThat( connection.getHoldability(), equalTo( RowsResultSet.CLOSE_CURSORS_AT_COMMIT ) );
  }

  @Override protected ThinConnection getTestObject() {
    return connection;
  }

  // PDI-14428
  @Test
  public void testCloseConnection() throws Exception {
    ThinConnection spyConnection = spy( connection );
    assertThat( spyConnection.isClosed(), is( false ) );
    ThinStatement thinStatement = new ThinStatement( spyConnection );
    verify( spyConnection, times( 1 ) ).registerStatement( thinStatement );
    spyConnection.close();
    assertThat( spyConnection.isClosed(), is( true ) );
    verify( (ConnectionAbortingSupport) clientService, times( 1 ) ).disconnect();
    verify( spyConnection, times( 1 ) ).closeAllOpenStatements();
  }

  @Test( expected = SQLException.class )
  public void testCloseConnectionException() throws Exception {
    SQLException expected = new SQLException();
    ThinConnection spyConnection = spy( connection );
    assertThat( spyConnection.isClosed(), is( false ) );

    doThrow( expected ).when( stm ).close();

    spyConnection.registerStatement( stm );

    try {
      spyConnection.close();
      fail( "Should fail" );
    } catch( Exception e ) {
      assertEquals( e, expected );
      throw e;
    }
  }

  // PDI-14430 - Exception Testing
  @Test
  public void testCommitNotSupportedExceptions() throws SQLException {
    expectedException.expect( SQLFeatureNotSupportedException.class );
    expectedException.expectMessage( noBangMatcher );
    connection.commit();
  }

  @Test
  public void testArrayNotSupportedExceptions() throws SQLException {
    expectedException.expect( SQLFeatureNotSupportedException.class );
    expectedException.expectMessage( noBangMatcher );
    connection.createArrayOf( "Abc", new String[3] );
  }

  @Test
  public void testCreateBlobNotSupportedExceptions() throws SQLException {
    expectedException.expect( SQLFeatureNotSupportedException.class );
    expectedException.expectMessage( noBangMatcher );
    connection.createBlob();
  }

  @Test
  public void testCreateClobNotSupportedExceptions() throws SQLException {
    expectedException.expect( SQLFeatureNotSupportedException.class );
    expectedException.expectMessage( noBangMatcher );
    connection.createClob();
  }

  @Test
  public void testCreateNClobNotSupportedExceptions() throws SQLException {
    expectedException.expect( SQLFeatureNotSupportedException.class );
    expectedException.expectMessage( noBangMatcher );
    connection.createNClob();
  }

  @Test
  public void testCreateSQLXMLNotSupportedExceptions() throws SQLException {
    expectedException.expect( SQLFeatureNotSupportedException.class );
    expectedException.expectMessage( noBangMatcher );
    connection.createSQLXML();
  }

  @Test
  public void testCreateStructNotSupportedExceptions() throws SQLException {
    expectedException.expect( SQLFeatureNotSupportedException.class );
    expectedException.expectMessage( noBangMatcher );
    connection.createStruct( "foo", new String[3] );
  }

  @Test
  public void testGetClientInfoNotSupportedExceptions() throws SQLException {
    expectedException.expect( SQLFeatureNotSupportedException.class );
    expectedException.expectMessage( noBangMatcher );
    connection.getClientInfo( "foo" );
  }

  @Test
  public void testNativeSQLNotSupportedExceptions() throws SQLException {
    expectedException.expect( SQLFeatureNotSupportedException.class );
    expectedException.expectMessage( noBangMatcher );
    connection.nativeSQL( "select *" );
  }

  @Test
  public void testPrepareCallNotSupportedExceptions() throws SQLException {
    expectedException.expect( SQLFeatureNotSupportedException.class );
    expectedException.expectMessage( noBangMatcher );
    connection.prepareCall( "select *" );
  }

  @Test
  public void testSavepointCallNotSupportedExceptions() throws SQLException {
    expectedException.expect( SQLFeatureNotSupportedException.class );
    expectedException.expectMessage( noBangMatcher );
    connection.setSavepoint();
  }

  @Test
  public void testAbortConnectionNotSupportedExceptions() throws SQLException {
    expectedException.expect( SQLFeatureNotSupportedException.class );
    expectedException.expectMessage( noBangMatcher );
    connection.abort( null );
  }

  @Test
  public void testNetworkTimeoutNotSupportedExceptions() throws SQLException {
    expectedException.expect( SQLFeatureNotSupportedException.class );
    expectedException.expectMessage( noBangMatcher );
    connection.getNetworkTimeout( );
  }

  @Test
  public void testGetLocalClientException() throws SQLException {
    connection.localClient = null;
    expectedException.expect( SQLException.class );
    expectedException.expectMessage( noBangMatcher );
    connection.getLocalClient();
  }

}
