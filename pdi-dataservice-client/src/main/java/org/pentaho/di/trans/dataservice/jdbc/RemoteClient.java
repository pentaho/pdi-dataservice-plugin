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

import com.google.common.base.CharMatcher;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.util.HttpClientUtil;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.dataservice.client.ConnectionAbortingSupport;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;
import org.pentaho.di.trans.dataservice.jdbc.api.IThinServiceInformation;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.DataInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

class RemoteClient implements IDataServiceClientService, ConnectionAbortingSupport {

  private static final String SQL = "SQL";
  private static final String MAX_ROWS = "MaxRows";
  private static final String WINDOW_ROW_SIZE = "WindowSize";
  private static final String WINDOW_TIME = "WindowTime";
  private static final String WINDOW_RATE = "UpdateRate";
  private static final String CONTENT_CHARSET = "utf-8";
  private static final int MAX_SQL_LENGTH = 7500;

  private final ThinConnection connection;
  private final HttpClient client;
  private final HttpClientContext context;
  private DocumentBuilderFactory docBuilderFactory;
  private static final String SERVICE_PATH = "/sql/";
  private final CopyOnWriteArrayList<HttpPost> activeMethods = new CopyOnWriteArrayList<HttpPost>();

  RemoteClient( ThinConnection connection, HttpClient client, HttpClientContext context ) {
    this.connection = connection;
    this.client = client;
    this.context = context;
  }

  @Override
  public DataInputStream query( String sql, int maxRows ) throws SQLException {
    HttpPost method = null;
    try {
      String url = connection.constructUrl( SERVICE_PATH );
      method = new HttpPost( url );

      method.getParams().setParameter( "http.socket.timeout", 0 );

      ArrayList<NameValuePair> postParameters = new ArrayList<NameValuePair>();
      // Kept in for backwards compatibility, but should be removed in next major release
      if ( sql.length() < MAX_SQL_LENGTH ) {
        method.addHeader( new BasicHeader( SQL, CharMatcher.anyOf( "\n\r" ).collapseFrom( sql, ' ' ) ) );
        method.addHeader( new BasicHeader( MAX_ROWS, Integer.toString( maxRows ) ) );
      }

      for ( Map.Entry<String, String> parameterEntry : connection.getParameters().entrySet() ) {
        postParameters.add( new BasicNameValuePair( parameterEntry.getKey(), parameterEntry.getValue() ) );
      }

      postParameters.add( new BasicNameValuePair( SQL, CharMatcher.anyOf( "\n\r" ).collapseFrom( sql, ' ' ) ) );
      postParameters.add( new BasicNameValuePair( MAX_ROWS, Integer.toString( maxRows ) ) );

      if ( !Strings.isNullOrEmpty( connection.getDebugTransFilename() ) ) {
        postParameters.add( new BasicNameValuePair( ThinConnection.ARG_DEBUGTRANS, connection.getDebugTransFilename() ) );
      }

      method.setEntity( new UrlEncodedFormEntity( postParameters, CONTENT_CHARSET ) );

      activeMethods.add( method );
      HttpResponse httpResponse = execMethod( method );
      return new DataInputStream( HttpClientUtil.responseToInputStream( httpResponse ) );
    } catch ( Exception e ) {
      throw serverException( e );
    } finally {
      activeMethods.remove( method );
    }
  }

  @Override
  public DataInputStream query( String sql, int maxRows, int windowSize, long windowMillis,
                                long windowRate )
          throws SQLException {
    HttpPost method = null;
    try {
      String url = connection.constructUrl( SERVICE_PATH );
      method = new HttpPost( url );

      method.getParams().setParameter( "http.socket.timeout", 0 );

      ArrayList<NameValuePair> postParameters = new ArrayList<NameValuePair>();
      // Kept in for backwards compatibility, but should be removed in next major release
      if ( sql.length() < MAX_SQL_LENGTH ) {
        method.addHeader( new BasicHeader( SQL, CharMatcher.anyOf( "\n\r" ).collapseFrom( sql, ' ' ) ) );
        method.addHeader( new BasicHeader( MAX_ROWS, Integer.toString( maxRows ) ) );
        method.addHeader( new BasicHeader( WINDOW_ROW_SIZE, Integer.toString( windowSize ) ) );
        method.addHeader( new BasicHeader( WINDOW_TIME, Long.toString( windowMillis ) ) );
        method.addHeader( new BasicHeader( WINDOW_RATE, Long.toString( windowRate ) ) );
      }

      for ( Map.Entry<String, String> parameterEntry : connection.getParameters().entrySet() ) {
        postParameters.add( new BasicNameValuePair( parameterEntry.getKey(), parameterEntry.getValue() ) );
      }

      postParameters.add( new BasicNameValuePair( SQL, CharMatcher.anyOf( "\n\r" ).collapseFrom( sql, ' ' ) ) );
      postParameters.add( new BasicNameValuePair( MAX_ROWS, Integer.toString( maxRows ) ) );
      postParameters.add( new BasicNameValuePair( WINDOW_ROW_SIZE, Integer.toString( windowSize ) ) );
      postParameters.add( new BasicNameValuePair( WINDOW_TIME, Long.toString( windowMillis ) ) );
      postParameters.add( new BasicNameValuePair( WINDOW_RATE, Long.toString( windowRate ) ) );

      if ( !Strings.isNullOrEmpty( connection.getDebugTransFilename() ) ) {
        postParameters.add( new BasicNameValuePair( ThinConnection.ARG_DEBUGTRANS, connection.getDebugTransFilename() ) );
      }

      method.setEntity( new UrlEncodedFormEntity( postParameters, CONTENT_CHARSET ) );

      activeMethods.add( method );
      HttpResponse httpResponse = execMethod( method );
      return new DataInputStream( HttpClientUtil.responseToInputStream( httpResponse ) );
    } catch ( Exception e ) {
      throw serverException( e );
    } finally {
      activeMethods.remove( method );
    }
  }

  @Override public List<IThinServiceInformation> getServiceInformation() throws SQLException {
    List<IThinServiceInformation> services = Lists.newArrayList();

    try {
      String result = execService( "/listServices" );
      Document doc = XMLHandler.loadXMLString( createDocumentBuilder(), result );
      Node servicesNode = XMLHandler.getSubNode( doc, "services" );
      List<Node> serviceNodes = XMLHandler.getNodes( servicesNode, "service" );

      for ( Node serviceNode : serviceNodes ) {
        String name = XMLHandler.getTagValue( serviceNode, "name" );
        boolean streaming = Boolean.parseBoolean( XMLHandler.getTagValue( serviceNode, "streaming" ) );
        Node rowMetaNode = XMLHandler.getSubNode( serviceNode, RowMeta.XML_META_TAG );
        RowMetaInterface serviceFields = new RowMeta( rowMetaNode );
        ThinServiceInformation service = new ThinServiceInformation( name, streaming, serviceFields );
        services.add( service );
      }
    } catch ( Exception e ) {
      throw serverException( e );
    }

    return services;
  }

  @Override public ThinServiceInformation getServiceInformation( String name ) throws SQLException {
    try {
      String result = execService( "/listServices" );
      Document doc = XMLHandler.loadXMLString( createDocumentBuilder(), result );
      Node servicesNode = XMLHandler.getSubNode( doc, "services" );
      List<Node> serviceNodes = XMLHandler.getNodes( servicesNode, "service" );

      for ( Node serviceNode : serviceNodes ) {
        String serviceName = XMLHandler.getTagValue( serviceNode, "name" );
        boolean streaming = Boolean.parseBoolean( XMLHandler.getTagValue( serviceNode, "streaming" ) );
        if ( serviceName.equals( name ) ) {
          Node rowMetaNode = XMLHandler.getSubNode( serviceNode, RowMeta.XML_META_TAG );
          RowMetaInterface serviceFields = new RowMeta( rowMetaNode );
          return new ThinServiceInformation( serviceName, streaming, serviceFields );
        }
      }

    } catch ( Exception e ) {
      throw serverException( e );
    }

    return null;
  }

  @Override public List<String> getServiceNames( String serviceName ) throws SQLException {
    return getServiceNames();
  }

  @Override public List<String> getServiceNames() throws SQLException {
    List<String> serviceNames = new ArrayList<String>();
    try {
      String result = execService( "/listServices" );
      Document doc = XMLHandler.loadXMLString( createDocumentBuilder(), result );
      Node servicesNode = XMLHandler.getSubNode( doc, "services" );
      List<Node> serviceNodes = XMLHandler.getNodes( servicesNode, "service" );

      for ( Node serviceNode : serviceNodes ) {
        String serviceName = XMLHandler.getTagValue( serviceNode, "name" );
        serviceNames.add( serviceName );
      }
    } catch ( Exception e ) {
      throw serverException( e );
    }
    return serviceNames;
  }

  @Override
  public void disconnect() {
    for ( HttpPost method : activeMethods ) {
      method.abort();
    }
  }

  private DocumentBuilder createDocumentBuilder() throws ParserConfigurationException {
    if ( docBuilderFactory == null ) {
      docBuilderFactory = DocumentBuilderFactory.newInstance();
    }
    return docBuilderFactory.newDocumentBuilder();
  }

  String execService( String serviceAndArguments ) throws SQLException {
    try {
      String urlString = connection.constructUrl( serviceAndArguments );
      HttpGet method = new HttpGet( urlString );

      try {
        HttpResponse httpResponse = execMethod( method );
        return httpResponseToString( httpResponse );
      } finally {
        method.releaseConnection();
      }

    } catch ( Exception e ) {
      throw serverException( e );
    }
  }

  HttpResponse execMethod( HttpRequestBase method ) throws SQLException {
    HttpResponse httpResponse = null;
    try {
      httpResponse = context != null ? client.execute( method, context ) : client.execute( method );
      int result = httpResponse.getStatusLine().getStatusCode();

      if ( result == HttpStatus.SC_INTERNAL_SERVER_ERROR ) {
        throw new SQLException( "There was an error reading data from the server." );
      }

      if ( result == HttpStatus.SC_UNAUTHORIZED ) {
        throw new SQLException(
          "Nice try-but we couldn't log you in. Check your username and password and try again." );
      }

      if ( result != HttpStatus.SC_OK ) {
        throw new SQLException( httpResponseToString( httpResponse ) );
      }
    } catch ( IOException e ) {
      throw new SQLException(
        "You don't seem to be getting a connection to the server or you have closed it. Check the host and port you're using and make sure the sever is up and running." );
    }
    return httpResponse;
  }

  protected String httpResponseToString( HttpResponse httpResponse ) throws IOException {
    return HttpClientUtil.responseToString( httpResponse );
  }

  private static SQLException serverException( Exception e ) throws SQLException {
    Throwables.propagateIfPossible( e, SQLException.class );
    throw new SQLException( "Error connecting to server", e );
  }

  @Deprecated
  public void setRepository( Repository repository ) {
  }

  @Deprecated
  public void setMetaStore( IMetaStore metaStore ) {
  }
}
