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

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.pentaho.di.cluster.HttpUtil;
import org.pentaho.di.cluster.SlaveConnectionManager;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.trans.dataservice.client.DataServiceClientService;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class ThinConnection implements Connection {

  public static final String ARG_WEBAPPNAME = "webappname";
  public static final String ARG_PROXYHOSTNAME = "proxyhostname";
  public static final String ARG_PROXYPORT = "proxyport";
  public static final String ARG_NONPROXYHOSTS = "nonproxyhosts";
  public static final String ARG_DEBUGTRANS = "debugtrans";
  public static final String ARG_DEBUGLOG = "debuglog";
  public static final String ARG_ISSECURE = "secure";
  public static final String ARG_LOCAL = "local";

  public static DataServiceClientService localClient;
  public DataServiceClientService clientService;

  private String url;
  private String username;
  private String password;
  private String hostname;
  private String port;

  private String webAppName;
  private String proxyHostname;
  private String proxyPort;
  private String nonProxyHosts;
  private boolean isSecure;

  private String debugTransFilename;
  private boolean debuggingRemoteLog;

  private boolean isLocal;
  private SQLWarning warning;

  private ThinConnection() {
  }

  protected String constructUrl( String serviceAndArguments ) throws SQLException {
    try {
      return HttpUtil.constructUrl( new Variables(), hostname, port, webAppName,
        ThinDriver.SERVICE_NAME + serviceAndArguments, isSecure );
    } catch ( Exception e ) {
      Throwables.propagateIfPossible( e, SQLException.class );
      throw new SQLException( e );
    }
  }

  @Override
  public boolean isWrapperFor( Class<?> arg0 ) throws SQLException {
    return false;
  }

  @Override
  public <T> T unwrap( Class<T> arg0 ) throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {
    warning = null;
  }

  @Override
  public void close() throws SQLException {
    // TODO

  }

  @Override
  public void commit() throws SQLException {
    throw new SQLException( "Transactions are not supported by the thin Kettle JDBC driver" );
  }

  @Override
  public Array createArrayOf( String arg0, Object[] arg1 ) throws SQLException {
    throw new SQLException( "Arrays are not supported by the thin Kettle JDBC driver" );
  }

  @Override
  public Blob createBlob() throws SQLException {
    throw new SQLException( "Creating BLOBs is not supported by the thin Kettle JDBC driver" );
  }

  @Override
  public Clob createClob() throws SQLException {
    throw new SQLException( "Creating CLOBs is not supported by the thin Kettle JDBC driver" );
  }

  @Override
  public NClob createNClob() throws SQLException {
    throw new SQLException( "Creating NCLOBs is not supported by the thin Kettle JDBC driver" );
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    throw new SQLException( "Creating SQL XML is not supported by the thin Kettle JDBC driver" );
  }

  @Override
  public Statement createStatement() throws SQLException {
    return new ThinStatement( this );
  }

  @Override
  public Statement createStatement( int resultSetType, int resultSetConcurrency ) throws SQLException {
    return new ThinStatement( this );
  }

  @Override
  public Statement createStatement( int resultSetType, int resultSetConcurrency, int resultSetHoldability ) throws SQLException {
    return new ThinStatement( this );
  }

  @Override
  public Struct createStruct( String arg0, Object[] arg1 ) throws SQLException {
    throw new SQLException( "Creating structs is not supported by the thin Kettle JDBC driver" );
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    return true;
  }

  @Override
  public String getCatalog() throws SQLException {
    return null;
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    return null;
  }

  @Override
  public String getClientInfo( String arg0 ) throws SQLException {
    return null;
  }

  @Override
  public int getHoldability() throws SQLException {
    return 0;
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    return new ThinDatabaseMetaData( this );
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    return 0;
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    // TODO
    return null;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return warning;
  }

  @Override
  public boolean isClosed() throws SQLException {
    // TODO
    return false;
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return true; // always read-only
  }

  @Override
  public boolean isValid( int timeout ) throws SQLException {
    try {
      // Execute dummy query to ensure data services are working
      Statement statement = createStatement();
      try {
        return statement.executeQuery( "SELECT *" ).next();
      } finally {
        statement.close();
      }
    } catch ( Exception e ) {
      warning = new SQLWarning( e );
      return false;
    }
  }

  @Override
  public String nativeSQL( String arg0 ) throws SQLException {
    throw new SQLException( "Native SQL statements are not supported by the thin Kettle JDBC driver" );
  }

  @Override
  public CallableStatement prepareCall( String arg0 ) throws SQLException {
    throw new SQLException( "Perpared calls are not supported by the thin Kettle JDBC driver" );
  }

  @Override
  public CallableStatement prepareCall( String arg0, int arg1, int arg2 ) throws SQLException {
    throw new SQLException( "Perpared calls are not supported by the thin Kettle JDBC driver" );
  }

  @Override
  public CallableStatement prepareCall( String arg0, int arg1, int arg2, int arg3 ) throws SQLException {
    throw new SQLException( "Perpared calls are not supported by the thin Kettle JDBC driver" );
  }

  @Override
  public PreparedStatement prepareStatement( String sql ) throws SQLException {
    return new ThinPreparedStatement( this, sql );
  }

  @Override
  public PreparedStatement prepareStatement( String sql, int autoGeneratedKeys ) throws SQLException {
    return new ThinPreparedStatement( this, sql );
  }

  @Override
  public PreparedStatement prepareStatement( String sql, int[] columnIndex ) throws SQLException {
    return new ThinPreparedStatement( this, sql );
  }

  @Override
  public PreparedStatement prepareStatement( String sql, String[] columnNames ) throws SQLException {
    return new ThinPreparedStatement( this, sql );
  }

  @Override
  public PreparedStatement prepareStatement( String sql, int resultSetType, int resultSetConcurrency ) throws SQLException {
    return new ThinPreparedStatement( this, sql );
  }

  @Override
  public PreparedStatement prepareStatement( String sql, int resultSetType, int resultSetConcurrency,
    int resultSetHoldability ) throws SQLException {
    return new ThinPreparedStatement( this, sql );
  }

  @Override
  public void releaseSavepoint( Savepoint arg0 ) throws SQLException {
    throw new SQLException( "Transactions are not supported by the thin Kettle JDBC driver" );
  }

  @Override
  public void rollback() throws SQLException {
    throw new SQLException( "Transactions are not supported by the thin Kettle JDBC driver" );
  }

  @Override
  public void rollback( Savepoint arg0 ) throws SQLException {
    throw new SQLException( "Transactions are not supported by the thin Kettle JDBC driver" );
  }

  @Override
  public void setAutoCommit( boolean auto ) throws SQLException {
    // Ignore this one.
  }

  @Override
  public void setCatalog( String arg0 ) throws SQLException {
    // Ignore: we don't use catalogs
  }

  @Override
  public void setClientInfo( Properties arg0 ) throws SQLClientInfoException {
  }

  @Override
  public void setClientInfo( String arg0, String arg1 ) throws SQLClientInfoException {
  }

  @Override
  public void setHoldability( int arg0 ) throws SQLException {
  }

  @Override
  public void setReadOnly( boolean arg0 ) throws SQLException {
    // Ignore, always read-only
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    throw new SQLException( "Safepoints calls are not supported by the thin Kettle JDBC driver" );
  }

  @Override
  public Savepoint setSavepoint( String arg0 ) throws SQLException {
    throw new SQLException( "Safepoints calls are not supported by the thin Kettle JDBC driver" );
  }

  @Override
  public void setTransactionIsolation( int arg0 ) throws SQLException {
    throw new SQLException( "Transactions are not supported by the thin Kettle JDBC driver" );
  }

  @Override
  public void setTypeMap( Map<String, Class<?>> arg0 ) throws SQLException {
    // TODO

  }

  /**
   * @return the url
   */
  public String getUrl() {
    return url;
  }

  /**
   * @return the username
   */
  public String getUsername() {
    return username;
  }

  /**
   * @return the hostname
   */
  public String getHostname() {
    return hostname;
  }

  /**
   * @return the port
   */
  public String getPort() {
    return port;
  }

  /**
   * @return the webAppName
   */
  public String getWebAppName() {
    return webAppName;
  }

  /**
   * @return the proxyHostname
   */
  public String getProxyHostname() {
    return proxyHostname;
  }

  /**
   * @return the proxyPort
   */
  public String getProxyPort() {
    return proxyPort;
  }

  /**
   * @return the nonProxyHosts
   */
  public String getNonProxyHosts() {
    return nonProxyHosts;
  }

  /**
   * @return the debugTransFilename
   */
  public String getDebugTransFilename() {
    return debugTransFilename;
  }

  /**
   * @return the debuggingRemoteLog
   */
  public boolean isDebuggingRemoteLog() {
    return debuggingRemoteLog;
  }

  public void setSchema( String schema ) throws SQLException {
  }

  public String getSchema() throws SQLException {
    return null;
  }

  @Override
  public void abort( Executor executor ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Abort Connection not supported" );
  }

  @Override
  public void setNetworkTimeout( Executor executor, int milliseconds ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Network Timeout not supported" );
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    throw new SQLFeatureNotSupportedException( "Network Timeout not supported" );
  }

  public boolean isLocal() {
    return isLocal;
  }

  public void setLocal( boolean isLocal ) {
    this.isLocal = isLocal;
  }

  public boolean isSecure() {
    return isSecure;
  }

  public static DataServiceClientService getLocalClient() throws SQLException {
    if ( localClient == null ) {
      throw new SQLException( "Local client service is not installed" );
    }
    return ThinConnection.localClient;
  }

  public DataServiceClientService getClientService() {
    return clientService;
  }

  private void addCredentials( HttpClient client ) {
    UsernamePasswordCredentials credentials = new UsernamePasswordCredentials( username, password );
    AuthScope scope;
    if ( Strings.isNullOrEmpty( webAppName ) ) {
      scope = new AuthScope( hostname, Const.toInt( port, 80 ), "Kettle" );
    } else {
      scope = AuthScope.ANY;
      client.getParams().setAuthenticationPreemptive( true );
    }
    client.getState().setCredentials( scope, credentials );
  }

  private void setProxy( HttpClient client ) {
    if ( !Const.isEmpty( proxyHostname ) && !Const.isEmpty( proxyPort ) ) {
      // skip applying proxy if non-proxy host matches
      if ( !Const.isEmpty( nonProxyHosts ) && !Const.isEmpty( hostname ) && hostname.matches( nonProxyHosts ) ) {
        return;
      }
      client.getHostConfiguration().setProxy( proxyHostname, Integer.parseInt( proxyPort ) );
    }
  }

  public static class Builder {
    private final SlaveConnectionManager connectionManager;
    private final ThinConnection connection;

    public Builder( SlaveConnectionManager connectionManager ) {
      connection = new ThinConnection();
      this.connectionManager = connectionManager;
    }

    public Builder parseUrl(String url) throws SQLException {
      connection.url = url;
      try {
        int portColonIndex = url.indexOf( ':', ThinDriver.BASE_URL.length() );
        int kettleIndex = url.indexOf( ThinDriver.SERVICE_NAME, portColonIndex );

        connection.hostname = url.substring( ThinDriver.BASE_URL.length(), portColonIndex );
        connection.port = url.substring( portColonIndex + 1, kettleIndex );

        int startIndex = url.indexOf( '?', kettleIndex ) + 1;
        Map<String, String> arguments = Maps.newHashMap();
        if ( startIndex > 0 ) {
          String path = url.substring( startIndex );
          String[] args = path.split( "&" );
          for ( String arg : args ) {
            String[] parts = arg.split( "=" );
            if ( parts.length == 2 ) {
              arguments.put( parts[0], parts[1] );
            }
          }
        }

        extractProperties( arguments );
      } catch ( Exception e ) {
        throw new SQLException( "Invalid connection URL.", e );
      }

      return this;
    }

    public Builder readProperties( Properties properties ) {
      connection.username = properties.getProperty( "user" );
      connection.password = properties.getProperty( "password" );
      return this;
    }

    private void extractProperties( Map<String, String> arguments ) {
      connection.webAppName = arguments.get( ARG_WEBAPPNAME );
      connection.proxyHostname = arguments.get( ARG_PROXYHOSTNAME );
      connection.proxyPort = arguments.get( ARG_PROXYPORT );
      connection.nonProxyHosts = arguments.get( ARG_NONPROXYHOSTS );
      connection.debugTransFilename = arguments.get( ARG_DEBUGTRANS );
      connection.debuggingRemoteLog = "true".equalsIgnoreCase( arguments.get( ARG_DEBUGLOG ) );
      connection.isSecure = "true".equalsIgnoreCase( arguments.get( ARG_ISSECURE ) );
      connection.isLocal = "true".equalsIgnoreCase( arguments.get( ARG_LOCAL ) );
    }

    private RemoteClient createRemoteClient() {
      HttpClient client = connectionManager.createHttpClient();
      connection.addCredentials( client );
      connection.setProxy( client );

      client.getHttpConnectionManager().getParams().setConnectionTimeout( 0 );
      client.getHttpConnectionManager().getParams().setSoTimeout( 0 );

      return new RemoteClient( connection, client );
    }

    public ThinConnection build( DataServiceClientService clientService ) throws SQLException {
      connection.clientService = clientService;
      return connection;
    }

    public ThinConnection build() throws SQLException {
      return build( connection.isLocal ? ThinConnection.getLocalClient() : createRemoteClient() );
    }
  }
}
