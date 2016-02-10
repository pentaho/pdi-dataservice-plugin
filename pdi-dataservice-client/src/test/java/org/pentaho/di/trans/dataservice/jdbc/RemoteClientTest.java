/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.io.Resources;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.lang.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.io.DataInputStream;
import java.sql.SQLException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author nhudak
 */
@RunWith( MockitoJUnitRunner.class )
public class RemoteClientTest {

  @Mock ThinConnection connection;
  @Mock HttpClient httpClient;
  @Mock HttpMethod execMethod;
  @Captor ArgumentCaptor<HttpMethod> httpMethodCaptor;
  RemoteClient remoteClient;

  @Before
  public void setUp() throws Exception {
    remoteClient = new RemoteClient( connection, httpClient ) {
      // Intercept execMethod so we can inject our mock response streams
      @Override protected HttpMethod execMethod( HttpMethod method ) throws SQLException {
        super.execMethod( method );
        return execMethod;
      }
    };
    when( connection.constructUrl( anyString() ) ).then( new Answer<String>() {
      @Override public String answer( InvocationOnMock invocation ) throws Throwable {
        return "http://localhost:9080/pentaho-di/kettle" + invocation.getArguments()[0];
      }
    } );
  }

  @Test
  public void testQuery() throws Exception {
    String sql = "SELECT * FROM myService\nWHERE id = 3";
    String debugTrans = "/tmp/genTrans.ktr";
    int maxRows = 200;

    when( connection.getDebugTransFilename() ).thenReturn( debugTrans );
    when( connection.getParameters() ).thenReturn( ImmutableMap.of( "PARAMETER_ECHO", "hello world" ) );
    when( connection.isDebuggingRemoteLog() ).thenReturn( true );

    when( httpClient.executeMethod( isA( PostMethod.class ) ) ).thenReturn( 200 );

    MockDataInput mockDataInput = new MockDataInput();
    mockDataInput.writeUTF( "Query Response" );

    when( execMethod.getResponseBodyAsStream() ).thenReturn( mockDataInput.toDataInputStream() );

    DataInputStream queryResponse = remoteClient.query( sql, maxRows );

    verify( httpClient ).executeMethod( httpMethodCaptor.capture() );
    PostMethod httpMethod = (PostMethod) httpMethodCaptor.getValue();

    assertThat( httpMethod.getURI().toString(), equalTo( "http://localhost:9080/pentaho-di/kettle/sql/" ) );
    assertThat( httpMethod.getRequestHeader( "SQL" ).getValue(), equalTo( "SELECT * FROM myService WHERE id = 3" ) );
    assertThat( httpMethod.getRequestHeader( "MaxRows" ).getValue(), equalTo( "200" ) );
    assertThat( httpMethod.getParameter( "SQL" ).getValue(), equalTo( "SELECT * FROM myService WHERE id = 3" ) );
    assertThat( httpMethod.getParameter( "MaxRows" ).getValue(), equalTo( "200" ) );

    assertThat( httpMethod.getParameter( "debugtrans" ).getValue(), equalTo( debugTrans ) );
    assertThat( httpMethod.getParameter( "debuglog" ).getValue(), equalTo( "true" ) );
    assertThat( httpMethod.getParameter( "PARAMETER_ECHO" ).getValue(), equalTo( "hello world" ) );

    assertThat( queryResponse.readUTF(), equalTo( "Query Response" ) );
  }

  @Test
  public void testLargeQuery() throws Exception {
    String sql = "SELECT * FROM myService\nWHERE id = 3 /" + StringUtils.repeat( "*", 8000 ) + "/";

    when( connection.getDebugTransFilename() ).thenReturn( null );
    when( connection.getParameters() ).thenReturn( ImmutableMap.<String, String>of() );
    when( connection.isDebuggingRemoteLog() ).thenReturn( true );

    when( httpClient.executeMethod( isA( PostMethod.class ) ) ).thenReturn( 200 );

    MockDataInput mockDataInput = new MockDataInput();
    mockDataInput.writeUTF( "Query Response" );

    when( execMethod.getResponseBodyAsStream() ).thenReturn( mockDataInput.toDataInputStream() );

    DataInputStream queryResponse = remoteClient.query( sql, 200 );

    verify( httpClient ).executeMethod( httpMethodCaptor.capture() );
    PostMethod httpMethod = (PostMethod) httpMethodCaptor.getValue();

    assertThat( httpMethod.getURI().toString(), equalTo( "http://localhost:9080/pentaho-di/kettle/sql/" ) );
    assertThat( httpMethod.getRequestHeader( "SQL" ), is( nullValue() ) );
    assertThat( httpMethod.getRequestHeader( "MaxRows" ), is( nullValue() ) );
    assertThat( httpMethod.getParameter( "SQL" ).getValue(),
        equalTo( "SELECT * FROM myService WHERE id = 3 /" + StringUtils.repeat( "*", 8000 ) + "/" ) );
    assertThat( httpMethod.getParameter( "MaxRows" ).getValue(), equalTo( "200" ) );

    assertThat( queryResponse.readUTF(), equalTo( "Query Response" ) );
  }

  @Test
  public void testGetServiceInformation() throws Exception {
    String url = "http://localhost:9080/pentaho-di/kettle/listServices";
    String xml = Resources.toString( ClassLoader.getSystemResource( "jdbc/listServices.xml" ), Charsets.UTF_8 );
    when( httpClient.executeMethod( isA( GetMethod.class ) ) ).thenReturn( 200 );
    when( execMethod.getResponseBodyAsString() ).thenReturn( xml );

    ThinServiceInformation serviceInformation = Iterables.getOnlyElement( remoteClient.getServiceInformation() );

    verify( httpClient ).executeMethod( httpMethodCaptor.capture() );
    assertThat( httpMethodCaptor.getValue().getURI().toString(), equalTo( url ) );

    assertThat( serviceInformation.getName(), is( "sequence" ) );
    assertThat( serviceInformation.getServiceFields().getFieldNames(), arrayContaining( "valuename" ) );
  }

  @Test
  public void testExecMethod() throws Exception {
    ImmutableList<Integer> statusCodes = ImmutableList.of( 500, 401, 404 );
    when( execMethod.getResponseBodyAsString() ).thenReturn( "kettle status" );

    for ( Integer statusCode : statusCodes ) {
      when( httpClient.executeMethod( any( HttpMethod.class ) ) ).thenReturn( statusCode );
      try {
        remoteClient.execMethod( execMethod );
        fail( "Expected an exception from response code" + statusCode );
      } catch ( SQLException e ) {
        assertThat( statusCode + " exception", e.getMessage(), not( emptyOrNullString() ) );
      }
    }

    when( httpClient.executeMethod( any( HttpMethod.class ) ) ).thenReturn( 200 );
    assertThat( remoteClient.execService( "/status" ), equalTo( "kettle status" ) );
  }

}
