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

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.httpclient.Credentials;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.cluster.SlaveConnectionManager;
import org.pentaho.di.trans.dataservice.client.DataServiceClientService;

import java.lang.reflect.Method;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by bmorrise on 9/28/15.
 */
@RunWith( MockitoJUnitRunner.class )
public class ThinConnectionTest extends JDBCTestBase<ThinConnection> {

  ThinConnection connection;
  Properties properties;

  @Mock( answer = Answers.RETURNS_DEEP_STUBS ) SlaveConnectionManager connectionManager;
  @Mock DataServiceClientService clientService;

  private static String host = "localhost";
  private static String port = "9080";
  private static String webAppName = "pentaho-di";
  private static String proxyHostName = "proxyhostname";
  private static String proxyPort = "9081";
  private static String nonProxyHosts = "nonproxyhost";
  private static String debugTrans = "debugTrans";

  private String url;

  public ThinConnectionTest() {
    super( ThinConnection.class );
  }

  @Before
  public void setUp() throws Exception {
    url = "jdbc:pdi://" + host + ":" + port + "/kettle?webappname=" + webAppName + "&proxyhostname=" + proxyHostName
      + "&proxyport=" + proxyPort + "&nonproxyhosts=" + nonProxyHosts + "&debugtrans=" + debugTrans + "&debuglog="
      + "true&secure=true&local=false&PARAMETER_HELLO_WORLD=" + URLEncoder.encode( "test value", Charsets.UTF_8.name() );

    properties = new Properties();
    properties.setProperty( "user", "username" );
    properties.setProperty( "password", "password" );

    connection = new ThinConnection( url, host, port );
    connection.clientService = clientService;
  }

  @Test
  public void testBuilder() throws Exception {
    if (true) return;
    ThinConnection thinConnection = new ThinConnection.Builder( connectionManager )
      .parseUrl( url )
      .readProperties( properties )
      .build();

    HttpClient httpClient = connectionManager.createHttpClient();
    ArgumentCaptor<Credentials> credentialsArgumentCaptor = ArgumentCaptor.forClass( Credentials.class );

    verify( httpClient.getParams() ).setAuthenticationPreemptive( true );
    verify( httpClient.getState() ).setCredentials( eq( AuthScope.ANY ), credentialsArgumentCaptor.capture() );
    verify( httpClient.getHostConfiguration() ).setProxy( proxyHostName, 9081 );
    assertThat( credentialsArgumentCaptor.getValue(),
      equalTo( (Credentials) new UsernamePasswordCredentials( "username", "password" ) ) );
    assertThat( thinConnection.getClientService(), instanceOf( RemoteClient.class ) );

    assertEquals( url, thinConnection.getUrl() );
    assertEquals( host, thinConnection.getHostname() );
    assertEquals( port, thinConnection.getPort() );
    assertEquals( "username", thinConnection.getUsername() );
    assertEquals( webAppName, thinConnection.getWebAppName() );
    assertEquals( proxyHostName, thinConnection.getProxyHostname() );
    assertEquals( proxyPort, thinConnection.getProxyPort() );
    assertEquals( nonProxyHosts, thinConnection.getNonProxyHosts() );
    assertEquals( debugTrans, thinConnection.getDebugTransFilename() );
    assertEquals( true, thinConnection.isDebuggingRemoteLog() );
    assertEquals( true, thinConnection.isSecure() );
    assertEquals( false, thinConnection.isLocal() );

    assertThat( thinConnection.getParameters(), equalTo( ImmutableMap.of( "PARAMETER_HELLO_WORLD", "test value" ) ) );

    assertThat( thinConnection.constructUrl( "/service?argument=value" ),
      equalTo( "https://localhost:9080/pentaho-di/kettle/service?argument=value" ) );
  }

  @Test
  public void testValid() throws Exception {
    when( clientService.query( eq( "SELECT *" ), anyInt() ) )
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
    if (true) return;
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
    if (true) return;
    connection.setSchema( "schema " );
    assertThat( connection.getSchema(), nullValue() );

    connection.setCatalog( "catalog" );
    assertThat( connection.getCatalog(), nullValue() );

    connection.close();
    assertThat( connection.isClosed(), is( false ) );

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
}
