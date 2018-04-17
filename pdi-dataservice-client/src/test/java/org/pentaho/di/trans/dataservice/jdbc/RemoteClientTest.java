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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.io.Resources;
import org.apache.commons.lang.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;
import org.pentaho.di.trans.dataservice.jdbc.api.IThinServiceInformation;

import java.io.IOException;
import java.net.URLDecoder;
import java.sql.SQLException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertTrue;
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

  @Mock
  private ThinConnection connection;
  @Mock
  private HttpClient httpClient;
  @Mock
  private HttpGet execMethod;
  @Mock
  private HttpClientContext context;
  @Mock
  private HttpEntity entity;
  @Mock
  private StatusLine statusLine;
  @Mock
  private HttpResponse response;

  @Captor
  private ArgumentCaptor<HttpRequestBase> httpMethodCaptor;
  private RemoteClientMock remoteClient;

  @Captor
  private ArgumentCaptor<HttpClientContext> httpContextCaptor;

  @Before
  public void setUp() throws Exception {
    remoteClient = new RemoteClientMock( connection, httpClient, context );
    when( connection.constructUrl( anyString() ) ).then( new Answer<String>() {
      @Override public String answer( InvocationOnMock invocation ) throws Throwable {
        return "http://localhost:8080/pentaho/kettle" + invocation.getArguments()[ 0 ];
      }
    } );
  }

  private void testQueryBefore() throws Exception {
    String debugTrans = "/tmp/genTrans.ktr";

    when( connection.getDebugTransFilename() ).thenReturn( debugTrans );
    when( connection.getParameters() ).thenReturn( ImmutableMap.of( "PARAMETER_ECHO", "hello world" ) );

    when( response.getStatusLine() ).thenReturn( statusLine );
    when( statusLine.getStatusCode() ).thenReturn( 200 );
    when( response.getEntity() ).thenReturn( entity );

    when( httpClient.execute( any( HttpUriRequest.class ), any( HttpContext.class ) ) ).thenReturn( response );

    MockDataInput mockDataInput = new MockDataInput();
    mockDataInput.writeUTF( "Query Response" );
  }

  private void testQueryAux() throws Exception {
    verify( httpClient ).execute( httpMethodCaptor.capture(), httpContextCaptor.capture() );
    HttpPost httpPost = (HttpPost) httpMethodCaptor.getValue();

    assertThat( httpPost.getURI().toString(), equalTo( "http://localhost:8080/pentaho/kettle/sql/" ) );
    assertThat( httpPost.getHeaders( "SQL" )[ 0 ].getValue(), equalTo( "SELECT * FROM myService WHERE id = 3" ) );
    assertThat( httpPost.getHeaders( "MaxRows" )[ 0 ].getValue(), equalTo( "200" ) );
    String actual = EntityUtils.toString( httpPost.getEntity() );
    EntityUtils.consume( httpPost.getEntity() );
    actual = URLDecoder.decode( actual, "UTF-8" );
    assertThat( actual, equalTo(
            "PARAMETER_ECHO=hello world&SQL=SELECT * FROM myService WHERE id = 3&MaxRows=200&debugtrans=/tmp/genTrans.ktr"
    ) );
    assertThat( (Integer) httpPost.getParams().getParameter( "http.socket.timeout" ), equalTo( 0 ) );
  }

  @Test
  public void testQuery() throws Exception {
    String sql = "SELECT * FROM myService\nWHERE id = 3";
    int maxRows = 200;

    testQueryBefore();

    remoteClient.query( sql, maxRows );

    testQueryAux();
  }

  @Test
  public void testQueryParams() throws Exception {
    String sql = "SELECT * FROM myService\nWHERE id = 3";
    int maxRows = 200;
    ImmutableMap<String, String> params = ImmutableMap.of();

    testQueryBefore();

    remoteClient.query( sql, maxRows, params );

    testQueryAux();
  }

  private void testStreamQueryAux() throws Exception {
    verify( httpClient ).execute( httpMethodCaptor.capture(), httpContextCaptor.capture() );
    HttpPost httpPost = (HttpPost) httpMethodCaptor.getValue();

    assertThat( httpPost.getURI().toString(), equalTo( "http://localhost:8080/pentaho/kettle/sql/" ) );
    assertThat( httpPost.getHeaders( "SQL" )[ 0 ].getValue(), equalTo( "SELECT * FROM myService WHERE id = 3" ) );
    assertThat( httpPost.getHeaders( "WindowMode" )[ 0 ].getValue(),
            equalTo( IDataServiceClientService.StreamingMode.ROW_BASED.toString() ) );
    assertThat( httpPost.getHeaders( "WindowSize" )[ 0 ].getValue(), equalTo( "1" ) );
    assertThat( httpPost.getHeaders( "WindowEvery" )[ 0 ].getValue(), equalTo( "2" ) );
    assertThat( httpPost.getHeaders( "WindowLimit" )[ 0 ].getValue(), equalTo( "3" ) );
    String actual = EntityUtils.toString( httpPost.getEntity() );
    EntityUtils.consume( httpPost.getEntity() );
    actual = URLDecoder.decode( actual, "UTF-8" );
    assertThat( actual, equalTo(
            "PARAMETER_ECHO=hello world&SQL=SELECT * FROM myService WHERE id = 3&WindowMode=ROW_BASED" +
                    "&WindowSize=1&WindowEvery=2&WindowLimit=3&debugtrans=/tmp/genTrans.ktr"
    ) );
    assertThat( (Integer) httpPost.getParams().getParameter( "http.socket.timeout" ), equalTo( 0 ) );
  }

  @Test
  public void testStreamQuery() throws Exception {
    String sql = "SELECT * FROM myService\nWHERE id = 3";

    testQueryBefore();

    remoteClient.query( sql, IDataServiceClientService.StreamingMode.ROW_BASED, 1, 2,
            3 );

    testStreamQueryAux();
  }

  @Test
  public void testStreamQueryParams() throws Exception {
    String sql = "SELECT * FROM myService\nWHERE id = 3";
    ImmutableMap<String, String> params = ImmutableMap.of();

    testQueryBefore();

    remoteClient.query( sql, IDataServiceClientService.StreamingMode.ROW_BASED, 1, 2,
            3, params );

    testStreamQueryAux();
  }

  @Test
  public void testLargeQuery() throws Exception {
    String sql = "SELECT * FROM myService\nWHERE id = 3 /" + StringUtils.repeat( "*", 8000 ) + "/";

    when( connection.getDebugTransFilename() ).thenReturn( null );
    when( connection.getParameters() ).thenReturn( ImmutableMap.<String, String>of() );

    when( response.getStatusLine() ).thenReturn( statusLine );
    when( statusLine.getStatusCode() ).thenReturn( 200 );
    when( response.getEntity() ).thenReturn( entity );
    when( httpClient.execute( isA( HttpPost.class ), isA( HttpClientContext.class ) ) ).thenReturn( response );

    MockDataInput mockDataInput = new MockDataInput();
    mockDataInput.writeUTF( "Query Response" );

    remoteClient.query( sql, 200 );

    verify( httpClient ).execute( httpMethodCaptor.capture(), httpContextCaptor.capture() );
    HttpPost httpMethod = (HttpPost) httpMethodCaptor.getValue();

    assertThat( httpMethod.getURI().toString(), equalTo( "http://localhost:8080/pentaho/kettle/sql/" ) );
    assertThat( httpMethod.getHeaders( "SQL" ), is( Matchers.<Header>emptyArray() ) );
    assertThat( httpMethod.getHeaders( "MaxRows" ), is( Matchers.<Header>emptyArray() ) );
    String actual = EntityUtils.toString( httpMethod.getEntity(), Charsets.UTF_8 );
    EntityUtils.consume( httpMethod.getEntity() );
    actual = URLDecoder.decode( actual, "UTF-8" );
    assertTrue(
      actual.contains( "SQL=SELECT * FROM myService WHERE id = 3 /" + StringUtils.repeat( "*", 8000 ) + "/" ) );
    assertTrue( actual.contains( "MaxRows=200" ) );
  }

  @Test
  public void testLargeStreamQuery() throws Exception {
    String sql = "SELECT * FROM myService\nWHERE id = 3 /" + StringUtils.repeat( "*", 8000 ) + "/";

    when( connection.getDebugTransFilename() ).thenReturn( null );
    when( connection.getParameters() ).thenReturn( ImmutableMap.<String, String>of() );

    when( response.getStatusLine() ).thenReturn( statusLine );
    when( statusLine.getStatusCode() ).thenReturn( 200 );
    when( response.getEntity() ).thenReturn( entity );
    when( httpClient.execute( isA( HttpPost.class ), isA( HttpClientContext.class ) ) ).thenReturn( response );

    MockDataInput mockDataInput = new MockDataInput();
    mockDataInput.writeUTF( "Query Response" );

    remoteClient.query( sql, IDataServiceClientService.StreamingMode.ROW_BASED,
            1, 2, 3 );

    verify( httpClient ).execute( httpMethodCaptor.capture(), httpContextCaptor.capture() );
    HttpPost httpMethod = (HttpPost) httpMethodCaptor.getValue();

    assertThat( httpMethod.getURI().toString(), equalTo( "http://localhost:8080/pentaho/kettle/sql/" ) );
    assertThat( httpMethod.getHeaders( "SQL" ), is( Matchers.<Header>emptyArray() ) );
    assertThat( httpMethod.getHeaders( "WindowMode" ), is( Matchers.<Header>emptyArray() ) );
    assertThat( httpMethod.getHeaders( "WindowSize" ), is( Matchers.<Header>emptyArray() ) );
    assertThat( httpMethod.getHeaders( "WindowEvery" ), is( Matchers.<Header>emptyArray() ) );
    assertThat( httpMethod.getHeaders( "WindowLimit" ), is( Matchers.<Header>emptyArray() ) );

    String actual = EntityUtils.toString( httpMethod.getEntity(), Charsets.UTF_8 );
    EntityUtils.consume( httpMethod.getEntity() );
    actual = URLDecoder.decode( actual, "UTF-8" );
    assertTrue(
            actual.contains( "SQL=SELECT * FROM myService WHERE id = 3 /" + StringUtils.repeat( "*", 8000 ) + "/" ) );
    assertTrue( actual.contains( "WindowMode=ROW_BASED" ) );
    assertTrue( actual.contains( "WindowSize=1" ) );
    assertTrue( actual.contains( "WindowEvery=2" ) );
    assertTrue( actual.contains( "WindowLimit=3" ) );
  }

  @Test
  public void testGetServiceInformation() throws Exception {
    String url = "http://localhost:8080/pentaho/kettle/listServices";
    String xml = Resources.toString( ClassLoader.getSystemResource( "jdbc/listServices.xml" ), Charsets.UTF_8 );

    when( response.getStatusLine() ).thenReturn( statusLine );
    when( statusLine.getStatusCode() ).thenReturn( 200 );
    when( response.getEntity() ).thenReturn( entity );
    when( httpClient.execute( isA( HttpGet.class ), isA( HttpClientContext.class ) ) ).thenReturn( response );

    remoteClient.setResponse( xml );

    IThinServiceInformation serviceInformation = Iterables.getOnlyElement( remoteClient.getServiceInformation() );

    verify( httpClient ).execute( httpMethodCaptor.capture(), httpContextCaptor.capture() );
    assertThat( httpMethodCaptor.getValue().getURI().toString(), equalTo( url ) );

    assertThat( serviceInformation.getName(), is( "sequence" ) );
    assertThat( serviceInformation.isStreaming(), is( false ) );
    assertThat( serviceInformation.getServiceFields().getFieldNames(), arrayContaining( "valuename" ) );

    serviceInformation = remoteClient.getServiceInformation( "sequence" );

    assertThat( serviceInformation.getName(), is( "sequence" ) );
    assertThat( serviceInformation.isStreaming(), is( false ) );
    assertThat( serviceInformation.getServiceFields().getFieldNames(), arrayContaining( "valuename" ) );
  }

  @Test
  public void testExecMethod() throws Exception {
    ImmutableList<Integer> statusCodes = ImmutableList.of( 500, 401, 404 );
    remoteClient.setResponse( "kettle status" );

    for ( Integer statusCode : statusCodes ) {
      when( response.getStatusLine() ).thenReturn( statusLine );
      when( statusLine.getStatusCode() ).thenReturn( statusCode );
      when( response.getEntity() ).thenReturn( entity );
      when( httpClient.execute( isA( HttpGet.class ), isA( HttpClientContext.class ) ) ).thenReturn( response );
      try {
        remoteClient.execMethod( execMethod );
        fail( "Expected an exception from response code" + statusCode );
      } catch ( SQLException e ) {
        assertThat( statusCode + " exception", e.getMessage(), not( emptyOrNullString() ) );
      }
    }
    when( response.getStatusLine() ).thenReturn( statusLine );
    when( statusLine.getStatusCode() ).thenReturn( 200 );
    when( response.getEntity() ).thenReturn( entity );
    when( httpClient.execute( isA( HttpPost.class ), isA( HttpClientContext.class ) ) ).thenReturn( response );
    assertThat( remoteClient.execService( "/status" ), equalTo( "kettle status" ) );
  }

  private class RemoteClientMock extends RemoteClient {
    private String response;

    RemoteClientMock( ThinConnection connection, HttpClient client, HttpClientContext context ) {
      super( connection, client, context );
    }

    void setResponse( String response ) {
      this.response = response;
    }

    // Intercept execMethod so we can inject our mock response streams
    @Override
    protected String httpResponseToString( HttpResponse httpResponse ) throws IOException {
      return response;
    }

  }

}
