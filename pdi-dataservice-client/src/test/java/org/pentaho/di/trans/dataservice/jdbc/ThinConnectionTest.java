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

import java.sql.SQLException;
import java.util.Properties;

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
public class ThinConnectionTest {

  ThinConnection connection;
  Properties properties;

  @Mock( answer = Answers.RETURNS_DEEP_STUBS ) SlaveConnectionManager connectionManager;
  @Mock DataServiceClientService clientService;

  @Before
  public void setUp() throws Exception {
    properties = new Properties();
    properties.setProperty( "user", "username" );
    properties.setProperty( "password", "password" );

    connection = new ThinConnection.Builder( connectionManager ).build( clientService );
  }

  @Test
  public void testBuilder() throws Exception {
    String host = "localhost";
    String port = "9080";
    String webAppName = "pentaho-di";
    String proxyHostName = "proxyhostname";
    String proxyPort = "9081";
    String nonProxyHosts = "nonproxyhost";
    String debugTrans = "debugTrans";
    String debugLog = "true";
    String secure = "true";
    String local = "false";

    String
        url =
        "jdbc:pdi://" + host + ":" + port + "/kettle?webappname=" + webAppName + "&proxyhostname=" + proxyHostName
            + "&proxyport=" + proxyPort + "&nonproxyhosts=" + nonProxyHosts + "&debugtrans=" + debugTrans + "&debuglog="
            + debugLog + "&secure=" + secure + "&local=" + local;

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
}
