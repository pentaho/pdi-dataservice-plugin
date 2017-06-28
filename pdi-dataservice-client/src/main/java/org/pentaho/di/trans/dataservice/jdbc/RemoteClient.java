/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
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
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.dataservice.client.ConnectionAbortingSupport;
import org.pentaho.di.trans.dataservice.client.DataServiceClientService;
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

/**
 * @author nhudak
 */
class RemoteClient implements DataServiceClientService, ConnectionAbortingSupport {

  private static final String SQL = "SQL";
  private static final String MAX_ROWS = "MaxRows";
  private static final String CONTENT_CHARSET = "utf-8";
  private static final int MAX_SQL_LENGTH = 7500;

  private final ThinConnection connection;
  private final HttpClient client;
  private DocumentBuilderFactory docBuilderFactory;
  private static final String SERVICE_PATH = "/sql/";
  private final CopyOnWriteArrayList<PostMethod> activeMethods = new CopyOnWriteArrayList<PostMethod>();

  RemoteClient( ThinConnection connection, HttpClient client ) {
    this.connection = connection;
    this.client = client;
  }

  @Override public DataInputStream query( String sql, int maxRows ) throws SQLException {
    PostMethod method = null;
    try {
      String url = connection.constructUrl( SERVICE_PATH );
      method = new PostMethod( url );
      method.setDoAuthentication( true );

      method.getParams().setParameter( "http.socket.timeout", 0 );

      // Kept in for backwards compatibility, but should be removed in next major release
      if ( sql.length() < MAX_SQL_LENGTH ) {
        method.addRequestHeader( new Header( SQL, CharMatcher.anyOf( "\n\r" ).collapseFrom( sql, ' ' ) ) );
        method.addRequestHeader( new Header( MAX_ROWS, Integer.toString( maxRows ) ) );
      }
      method.addParameter( SQL, CharMatcher.anyOf( "\n\r" ).collapseFrom( sql, ' ' ) );
      method.addParameter( MAX_ROWS, Integer.toString( maxRows ) );

      for ( Map.Entry<String, String> parameterEntry : connection.getParameters().entrySet() ) {
        method.addParameter( parameterEntry.getKey(), parameterEntry.getValue() );
      }
      if ( !Strings.isNullOrEmpty( connection.getDebugTransFilename() ) ) {
        method.addParameter( ThinConnection.ARG_DEBUGTRANS, connection.getDebugTransFilename() );
      }
      method.getParams().setContentCharset( CONTENT_CHARSET );
      activeMethods.add( method );
      return new DataInputStream( execMethod( method ).getResponseBodyAsStream() );
    } catch ( Exception e ) {
      throw serverException( e );
    } finally {
      activeMethods.remove( method );
    }
  }

  @Override public List<ThinServiceInformation> getServiceInformation() throws SQLException {
    List<ThinServiceInformation> services = Lists.newArrayList();

    try {
      String result = execService( "/listServices" );
      Document doc = XMLHandler.loadXMLString( createDocumentBuilder(), result );
      Node servicesNode = XMLHandler.getSubNode( doc, "services" );
      List<Node> serviceNodes = XMLHandler.getNodes( servicesNode, "service" );

      for ( Node serviceNode : serviceNodes ) {
        String name = XMLHandler.getTagValue( serviceNode, "name" );
        Node rowMetaNode = XMLHandler.getSubNode( serviceNode, RowMeta.XML_META_TAG );
        RowMetaInterface serviceFields = new RowMeta( rowMetaNode );
        ThinServiceInformation service = new ThinServiceInformation( name, serviceFields );
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
        if ( serviceName.equals( name ) ) {
          Node rowMetaNode = XMLHandler.getSubNode( serviceNode, RowMeta.XML_META_TAG );
          RowMetaInterface serviceFields = new RowMeta( rowMetaNode );
          return new ThinServiceInformation( serviceName, serviceFields );
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

  @Override public void disconnect() {
    for ( PostMethod method : activeMethods ) {
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
      HttpMethod method = new GetMethod( urlString );

      try {
        return execMethod( method ).getResponseBodyAsString();
      } finally {
        method.releaseConnection();
      }

    } catch ( Exception e ) {
      throw serverException( e );
    }
  }

  HttpMethod execMethod( HttpMethod method ) throws SQLException {
    try {
      int result = client.executeMethod( method );

      if ( result == 500 ) {
        throw new SQLException( "There was an error reading data from the server." );
      }

      if ( result == 401 ) {
        throw new SQLException(
          "Nice try-but we couldn't log you in. Check your username and password and try again." );
      }

      if ( result != 200 ) {
        throw new SQLException( method.getResponseBodyAsString() );
      }
    } catch ( IOException e ) {
      throw new SQLException(
        "You don't seem to be getting a connection to the server or you have closed it. Check the host and port you're using and make sure the sever is up and running." );
    }
    return method;
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
