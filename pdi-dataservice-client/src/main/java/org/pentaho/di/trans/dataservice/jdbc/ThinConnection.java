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

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.protocol.HttpClientContext;
import org.pentaho.di.cluster.SlaveConnectionManager;
import org.pentaho.di.core.database.BaseDatabaseMeta;
import org.pentaho.di.core.util.HttpClientManager;
import org.pentaho.di.core.util.HttpClientUtil;
import org.pentaho.di.trans.dataservice.client.ConnectionAbortingSupport;
import org.pentaho.di.trans.dataservice.client.DataServiceClientService;
import org.pentaho.di.trans.dataservice.jdbc.annotation.NotSupported;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;

public class ThinConnection extends ThinBase implements Connection {

  public static final String ARG_WEBAPPNAME = "webappname";
  public static final String ARG_PROXYHOSTNAME = "proxyhostname";
  public static final String ARG_PROXYPORT = "proxyport";
  public static final String ARG_NONPROXYHOSTS = "nonproxyhosts";
  public static final String ARG_DEBUGTRANS = "debugtrans";
  public static final String ARG_ISSECURE = "secure";
  public static final String ARG_LOCAL = "local";
  public static final String ARG_WEB_APPLICATION_NAME = BaseDatabaseMeta.ATTRIBUTE_PREFIX_EXTRA_OPTION
      + "KettleThin.webappname";

  public static DataServiceClientService localClient;
  private DataServiceClientService clientService;

  private String url;
  private URI baseURI;

  private String username;
  private String password;

  private String proxyHostname;
  private String proxyPort;
  private String nonProxyHosts;

  private String debugTransFilename;

  private ImmutableMap<String, String> parameters = ImmutableMap.of();

  /**
   * An array of currently open statements.
   * Copy-on-write used here to avoid ConcurrentModificationException when statements unregister themselves while we iterate over the list.
   */
  private final CopyOnWriteArrayList<ThinStatement> openStatements = new CopyOnWriteArrayList<ThinStatement>();

  protected ThinConnection() {
  }

  protected ThinConnection( String url, URI baseURI ) {
    this.url = url;
    this.baseURI = baseURI;
  }

  protected String constructUrl( String service ) throws SQLException {
    return baseURI.resolve( "./" + service ).toString();
  }

  @Override
  public void close() throws SQLException {
    //clean all resources
    if ( clientService instanceof ConnectionAbortingSupport ) {
      ( (ConnectionAbortingSupport) clientService ).disconnect();
    }
    closeAllOpenStatements();
    clientService = null;
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
    return clientService == null;
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
    return baseURI.getHost();
  }

  /**
   * @return the port
   */
  public int getPort() {
    return baseURI.getPort();
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
    return !( getClientService() instanceof RemoteClient );
  }

  public boolean isSecure() {
    return baseURI.getScheme().equals( "https" );
  }

  public static DataServiceClientService getLocalClient() throws SQLException {
    if ( localClient == null ) {
      throw new SQLException( "Local client service is not installed" );
    }
    return ThinConnection.localClient;
  }

  protected void setClientService( DataServiceClientService clientService ) {
    this.clientService = clientService;
  }

  public DataServiceClientService getClientService() {
    return Preconditions.checkNotNull( clientService, "Client Service not set for connection" );
  }

  private ThinConnection extractProperties( Map<String, String> arguments ) {
    proxyHostname = arguments.get( ARG_PROXYHOSTNAME );
    proxyPort = arguments.get( ARG_PROXYPORT );
    nonProxyHosts = arguments.get( ARG_NONPROXYHOSTS );
    debugTransFilename = arguments.get( ARG_DEBUGTRANS );

    parameters = ImmutableMap.copyOf( Maps.filterKeys( arguments, new Predicate<String>() {
      @Override public boolean apply( String input ) {
        return input.startsWith( "PARAMETER_" );
      }
    } ) );

    username = arguments.get( "user" );
    password = arguments.get( "password" );

    return this;
  }

  protected Builder createBuilder() {
    return new Builder();
  }

  public class Builder {
    private final HttpClientManager httpClientManager = HttpClientManager.getInstance();
    private final Map<String, String> arguments = Maps.newHashMap();
    private String url;
    private URI uri;

    @Deprecated
    public Builder( SlaveConnectionManager connectionManager ) {
    }

    public Builder() {
    }

    public Builder parseUrl( String url ) throws SQLException {
      this.url = url;
      try {
        // Remove 'jdbc:' prefix
        uri = URI.create( url.substring( 5 ) );

        if ( !Strings.isNullOrEmpty( uri.getQuery() ) ) {
          Map<String, String> queryParameters = Splitter.on( '&' ).withKeyValueSeparator( '=' ).split( uri.getQuery() );
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

    String decode( String s ) throws UnsupportedEncodingException {
      return URLDecoder.decode( s, Charsets.UTF_8.name() );
    }

    public Builder readProperties( Properties properties ) {
      arguments.putAll( Maps.fromProperties( properties ) );
      return this;
    }

    private RemoteClient createRemoteClient( ThinConnection connection ) {
      HttpClientManager.HttpClientBuilderFacade clientBuilder = httpClientManager.createBuilder();
      HttpClientContext clientContext = null;
      clientBuilder.setSocketTimeout( 0 );
      clientBuilder.setConnectionTimeout( 0 );
      String user = connection.username;
      String pass = connection.password;
      if ( StringUtils.isNotBlank( user ) ) {
        URI uri = connection.baseURI;
        clientBuilder.setCredentials( user, pass );
        clientContext = HttpClientUtil.createPreemptiveBasicAuthentication(
          uri.getHost(), uri.getPort(), user, pass, uri.getScheme() );
      }

      if ( StringUtils.isNotBlank( proxyHostname ) && StringUtils.isNotBlank( proxyPort )
        && ( StringUtils.isBlank( nonProxyHosts ) || ( StringUtils.isNotBlank( nonProxyHosts ) && !getHostname()
        .matches( nonProxyHosts ) ) ) ) {
        int proxyPort = Integer.parseInt( connection.proxyPort );
        clientBuilder.setProxy( proxyHostname, proxyPort );
        clientContext = HttpClientUtil.createPreemptiveBasicAuthentication( proxyHostname, proxyPort, user, pass );
      }

      return new RemoteClient( connection, clientBuilder.build(), clientContext );
    }

    public ThinConnection build() throws SQLException {
      boolean isLocal = "true".equalsIgnoreCase( arguments.get( ARG_LOCAL ) );

      ThinConnection connection = new ThinConnection( url, baseUri() ).extractProperties( arguments );
      connection.clientService = isLocal ? ThinConnection.getLocalClient() : createRemoteClient( connection );
      return connection;
    }

    private URI baseUri() throws SQLException {
      boolean isSecure = "true".equalsIgnoreCase( arguments.get( ARG_ISSECURE ) );

      String pathPrefix = "";
      if ( arguments.containsKey( ARG_WEBAPPNAME ) ) {
        ThinDriver.logger.warning( ARG_WEBAPPNAME + " is deprecated. "
          + "Please instead use the form jdbc:pdi://myserver.mydomain.com:8080/webAppName/kettle" );
        pathPrefix = "/" + arguments.get( ARG_WEBAPPNAME );
      }

      try {
        return new URI(
          isSecure ? "https" : "http",
          null,
          uri.getHost(),
          uri.getPort() > 0 ? uri.getPort() : isSecure ? 443 : 80,
          pathPrefix + Strings.nullToEmpty( uri.getPath() ) + '/',
          null,
          null
        );
      } catch ( URISyntaxException e ) {
        throw new SQLException( "Unable to create a connection", e );
      }
    }
  }

  /**
   * Register a Statement instance as open.
   *
   * @param stmt
   *            the ThinStatement instance to register
   */
  public void registerStatement( ThinStatement stmt ) {
    this.openStatements.addIfAbsent( stmt );
  }


  /**
   * Remove the given statement from the list of open statements
   *
   * @param stmt
   *            the ThinStatement instance to remove
   */
  public void unregisterStatement( ThinStatement stmt ) {
    this.openStatements.remove( stmt );
  }

  /**
   * Closes all currently open statements.
   *
   * @throws SQLException
   */
  public void closeAllOpenStatements() throws SQLException {
    SQLException postponedException = null;

    for ( ThinStatement stmt : this.openStatements ) {
      try {
        stmt.close();
      } catch ( SQLException sqlEx ) {
        postponedException = sqlEx; // throw it later, cleanup all statements first
      }
    }

    if ( postponedException != null ) {
      throw postponedException;
    }
  }
}
