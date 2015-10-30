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
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.pentaho.di.cluster.HttpUtil;
import org.pentaho.di.cluster.SlaveConnectionManager;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.trans.dataservice.client.DataServiceClientService;
import org.pentaho.di.trans.dataservice.jdbc.annotation.NotSupported;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import static com.google.common.base.Preconditions.checkNotNull;

public class ThinConnection extends ThinBase implements Connection {

  public static final String ARG_WEBAPPNAME = "webappname";
  public static final String ARG_PROXYHOSTNAME = "proxyhostname";
  public static final String ARG_PROXYPORT = "proxyport";
  public static final String ARG_NONPROXYHOSTS = "nonproxyhosts";
  public static final String ARG_DEBUGTRANS = "debugtrans";
  public static final String ARG_DEBUGLOG = "debuglog";
  public static final String ARG_ISSECURE = "secure";
  public static final String ARG_LOCAL = "local";

  public static DataServiceClientService localClient;
  protected DataServiceClientService clientService;

  private final String url;
  private final String hostname;
  private final String port;

  private String username;
  private String password;

  private String webAppName;
  private String proxyHostname;
  private String proxyPort;
  private String nonProxyHosts;
  private boolean isSecure;

  private String debugTransFilename;
  private boolean debuggingRemoteLog;

  private ImmutableMap<String, String> parameters = ImmutableMap.of();

  private boolean isLocal;

  protected ThinConnection( String url, String hostname, String port ) {
    this.url = url;
    this.hostname = hostname;
    this.port = port;
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
  public void close() throws SQLException {
    // TODO
  }

  @Override @NotSupported
  public void commit() throws SQLException {
    throw new SQLFeatureNotSupportedException( "Transactions are not supported by the thin Kettle JDBC driver" );
  }

  @Override @NotSupported
  public Array createArrayOf( String arg0, Object[] arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Arrays are not supported by the thin Kettle JDBC driver" );
  }

  @Override @NotSupported
  public Blob createBlob() throws SQLException {
    throw new SQLFeatureNotSupportedException( "Creating BLOBs is not supported by the thin Kettle JDBC driver" );
  }

  @Override @NotSupported
  public Clob createClob() throws SQLException {
    throw new SQLFeatureNotSupportedException( "Creating CLOBs is not supported by the thin Kettle JDBC driver" );
  }

  @Override @NotSupported
  public NClob createNClob() throws SQLException {
    throw new SQLFeatureNotSupportedException( "Creating NCLOBs is not supported by the thin Kettle JDBC driver" );
  }

  @Override @NotSupported
  public SQLXML createSQLXML() throws SQLException {
    throw new SQLFeatureNotSupportedException( "Creating SQL XML is not supported by the thin Kettle JDBC driver" );
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
  public Statement createStatement( int resultSetType, int resultSetConcurrency, int resultSetHoldability ) {
    return new ThinStatement( this );
  }

  @Override @NotSupported
  public Struct createStruct( String arg0, Object[] arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Creating structs is not supported by the thin Kettle JDBC driver" );
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
    return new Properties();
  }

  @Override @NotSupported
  public String getClientInfo( String arg0 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Client Info is not supported by the thin Kettle JDBC driver" );
  }

  @Override
  public int getHoldability() throws SQLException {
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
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
      setWarning( e );
      return false;
    }
  }

  @Override @NotSupported
  public String nativeSQL( String arg0 ) throws SQLException {
    throw new SQLFeatureNotSupportedException(
      "Native SQL statements are not supported by the thin Kettle JDBC driver" );
  }

  @Override @NotSupported
  public CallableStatement prepareCall( String arg0 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Perpared calls are not supported by the thin Kettle JDBC driver" );
  }

  @Override @NotSupported
  public CallableStatement prepareCall( String arg0, int arg1, int arg2 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Perpared calls are not supported by the thin Kettle JDBC driver" );
  }

  @Override @NotSupported
  public CallableStatement prepareCall( String arg0, int arg1, int arg2, int arg3 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Perpared calls are not supported by the thin Kettle JDBC driver" );
  }

  @Override
  public PreparedStatement prepareStatement( String sql ) throws SQLException {
    return new ThinPreparedStatement( this, sql );
  }

  @Override
  public PreparedStatement prepareStatement( String sql, int autoGeneratedKeys ) throws SQLException {
    return prepareStatement( sql );
  }

  @Override
  public PreparedStatement prepareStatement( String sql, int[] columnIndex ) throws SQLException {
    return prepareStatement( sql );
  }

  @Override
  public PreparedStatement prepareStatement( String sql, String[] columnNames ) throws SQLException {
    return prepareStatement( sql );
  }

  @Override
  public PreparedStatement prepareStatement( String sql, int resultSetType, int resultSetConcurrency )
    throws SQLException {
    return prepareStatement( sql );
  }

  @Override
  public PreparedStatement prepareStatement( String sql, int resultSetType, int resultSetConcurrency,
                                             int resultSetHoldability ) throws SQLException {
    return prepareStatement( sql );
  }

  @Override @NotSupported
  public void releaseSavepoint( Savepoint arg0 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Transactions are not supported by the thin Kettle JDBC driver" );
  }

  @Override @NotSupported
  public void rollback() throws SQLException {
    throw new SQLFeatureNotSupportedException( "Transactions are not supported by the thin Kettle JDBC driver" );
  }

  @Override @NotSupported
  public void rollback( Savepoint arg0 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Transactions are not supported by the thin Kettle JDBC driver" );
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
    // ignored
  }

  @Override
  public void setReadOnly( boolean arg0 ) throws SQLException {
    // Ignore, always read-only
  }

  @Override @NotSupported
  public Savepoint setSavepoint() throws SQLException {
    throw new SQLFeatureNotSupportedException( "Safepoints calls are not supported by the thin Kettle JDBC driver" );
  }

  @Override @NotSupported
  public Savepoint setSavepoint( String arg0 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Safepoints calls are not supported by the thin Kettle JDBC driver" );
  }

  @Override @NotSupported
  public void setTransactionIsolation( int arg0 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Transactions are not supported by the thin Kettle JDBC driver" );
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

  ImmutableMap<String, String> getParameters() {
    return parameters;
  }

  public void setSchema( String schema ) throws SQLException {
  }

  public String getSchema() throws SQLException {
    return null;
  }

  @Override @NotSupported
  public void abort( Executor executor ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Abort Connection not supported" );
  }

  @Override @NotSupported
  public void setNetworkTimeout( Executor executor, int milliseconds ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Network Timeout not supported" );
  }

  @Override @NotSupported
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
    return checkNotNull( clientService, "Client Service not set for connection" );
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

  private ThinConnection extractProperties( Map<String, String> arguments ) {
    webAppName = arguments.get( ARG_WEBAPPNAME );
    proxyHostname = arguments.get( ARG_PROXYHOSTNAME );
    proxyPort = arguments.get( ARG_PROXYPORT );
    nonProxyHosts = arguments.get( ARG_NONPROXYHOSTS );
    debugTransFilename = arguments.get( ARG_DEBUGTRANS );
    debuggingRemoteLog = "true".equalsIgnoreCase( arguments.get( ARG_DEBUGLOG ) );
    isSecure = "true".equalsIgnoreCase( arguments.get( ARG_ISSECURE ) );
    isLocal = "true".equalsIgnoreCase( arguments.get( ARG_LOCAL ) );

    parameters = ImmutableMap.copyOf( Maps.filterKeys( arguments, new Predicate<String>() {
      @Override public boolean apply( String input ) {
        return input.startsWith( "PARAMETER_" );
      }
    } ) );

    username = arguments.get( "user" );
    password = arguments.get( "password" );

    return this;
  }

  public static class Builder {
    private final SlaveConnectionManager connectionManager;
    private final Map<String, String> arguments = Maps.newHashMap();
    private String url;
    private String hostname;
    private String port;

    public Builder( SlaveConnectionManager connectionManager ) {
      this.connectionManager = connectionManager;
    }

    public Builder parseUrl( String url ) throws SQLException {
      this.url = url;
      try {
        int portColonIndex = url.indexOf( ':', ThinDriver.BASE_URL.length() );
        if ( portColonIndex < 0 ) {
          throw new SQLException( "Port is not defined: " + url );
        }
        int kettleIndex = url.indexOf( ThinDriver.SERVICE_NAME, portColonIndex );

        hostname = url.substring( ThinDriver.BASE_URL.length(), portColonIndex );
        port = url.substring( portColonIndex + 1, kettleIndex );

        int startIndex = url.indexOf( '?', kettleIndex ) + 1;
        if ( startIndex > 0 ) {
          String path = url.substring( startIndex );
          Map<String, String> queryParameters = Splitter.on( '&' ).withKeyValueSeparator( '=' ).split( path );
          for ( Map.Entry<String, String> parameterEntry : queryParameters.entrySet() ) {
            arguments.put( decode( parameterEntry.getKey() ), decode( parameterEntry.getValue() ) );
          }
        }
      } catch ( Exception e ) {
        Throwables.propagateIfPossible( e, SQLException.class );
        throw new SQLException( "Invalid connection URL: " + url, e );
      }

      return this;
    }

    static String decode( String s ) throws UnsupportedEncodingException {
      return URLDecoder.decode( s, Charsets.UTF_8.name() );
    }

    public Builder readProperties( Properties properties ) {
      arguments.putAll( Maps.fromProperties( properties ) );
      return this;
    }

    private RemoteClient createRemoteClient( ThinConnection connection ) {
      HttpClient client = connectionManager.createHttpClient();
      connection.addCredentials( client );
      connection.setProxy( client );

      client.getHttpConnectionManager().getParams().setConnectionTimeout( 0 );
      client.getHttpConnectionManager().getParams().setSoTimeout( 0 );

      return new RemoteClient( connection, client );
    }

    public ThinConnection build() throws SQLException {
      ThinConnection connection = new ThinConnection( url, hostname, port ).extractProperties( arguments );
      connection.clientService = connection.isLocal ? ThinConnection.getLocalClient() : createRemoteClient( connection );
      return connection;
    }
  }
}
