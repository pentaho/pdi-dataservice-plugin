/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package org.pentaho.di.trans.dataservice.jdbc;

import io.reactivex.Observer;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService.IStreamingParams;
import org.pentaho.di.trans.dataservice.jdbc.annotation.NotSupported;
import org.pentaho.di.trans.dataservice.jdbc.api.IThinStatement;

import java.io.DataInputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.List;

public class ThinStatement extends ThinBase implements IThinStatement {

  protected final ThinConnection connection;
  private final ThinResultFactory resultFactory;
  private ThinResultSet resultSet;

  protected int maxRows = -1;

  public ThinStatement( ThinConnection connection ) {
    this( connection, new ThinResultFactory() );
  }

  protected ThinStatement( ThinConnection connection, ThinResultFactory resultFactory ) {
    this.connection = connection;
    this.resultFactory = resultFactory;
    this.connection.registerStatement( this );
  }

  @Override @NotSupported
  public void addBatch( String arg0 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Batches not supported" );
  }

  @Override
  public void cancel() throws SQLException {
    if ( resultSet != null ) {
      resultSet.close();
    }
    connection.unregisterStatement( this );
  }

  @Override @NotSupported
  public void clearBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException( "Batch update statements are not supported by the thin Kettle JDBC driver" );
  }

  @Override
  public void close() throws SQLException {
    cancel();
  }

  @Override
  public boolean execute( String sql ) throws SQLException {
    return executeQuery( sql ) != null;
  }

  @Override
  public boolean execute( String sql, int autoGeneratedKeys ) throws SQLException {
    return executeQuery( sql ) != null;
  }

  @Override
  public boolean execute( String sql, int[] arg1 ) throws SQLException {
    return executeQuery( sql ) != null;
  }

  @Override
  public boolean execute( String sql, String[] arg1 ) throws SQLException {
    return executeQuery( sql ) != null;
  }

  @Override @NotSupported
  public int[] executeBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException( "Batch update statements are not supported by the thin Kettle JDBC driver" );
  }

  @Override
  public ResultSet executeQuery( String sql ) throws SQLException {
    DataInputStream dataInputStream = connection.getClientService().query( sql, maxRows, connection.getParameters() );
    resultSet = resultFactory.loadResultSet( dataInputStream, connection.getClientService() );
    resultSet.setStatement( this );
    return resultSet;
  }

  @Override
  public ResultSet executeQuery( String sql, IDataServiceClientService.StreamingMode windowMode,
                                long windowSize, long windowEvery,
                                long windowLimit ) throws SQLException {
    DataInputStream dataInputStream = connection.getClientService().query( sql, windowMode, windowSize,
            windowEvery, windowLimit, connection.getParameters() );
    resultSet = resultFactory.loadResultSet( dataInputStream, connection.getClientService() );
    resultSet.setStatement( this );
    return resultSet;
  }

  @Override @NotSupported
  public int executeUpdate( String sql ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "The thin Kettle JDBC driver is read-only" );
  }

  @Override @NotSupported
  public int executeUpdate( String sql, int arg1 ) throws SQLException {
    return executeUpdate( sql );
  }

  @Override @NotSupported
  public int executeUpdate( String sql, int[] arg1 ) throws SQLException {
    return executeUpdate( sql );
  }

  @Override @NotSupported
  public int executeUpdate( String sql, String[] arg1 ) throws SQLException {
    return executeUpdate( sql );
  }

  @Override
  public Connection getConnection() throws SQLException {
    return connection;
  }

  @Override
  public int getFetchDirection() throws SQLException {
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public int getFetchSize() throws SQLException {
    return 1;
  }

  @Override @NotSupported
  public ResultSet getGeneratedKeys() throws SQLException {
    throw new SQLFeatureNotSupportedException( "The thin Kettle JDBC driver is read-only" );
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    return 0;
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    if ( resultSet == null ) {
      throw new SQLException( "Statement is closed." );
    } else if ( resultSet.isLast() || resultSet.isClosed() ) {
      resultSet.close();
      return false;
    } else {
      return true;
    }
  }

  @Override
  public boolean getMoreResults( int current ) throws SQLException {
    if ( current != Statement.CLOSE_CURRENT_RESULT ) {
      throw new SQLFeatureNotSupportedException( "Multiple open result sets not supported" );
    }
    return getMoreResults();
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    return 0;
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    return resultSet;
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    return resultSet.getConcurrency();
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    return resultSet.getHoldability();
  }

  @Override
  public int getResultSetType() throws SQLException {
    return resultSet.getType();
  }

  @Override
  public int getUpdateCount() throws SQLException {
    return 0;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return resultSet.isClosed();
  }

  @Override
  public boolean isPoolable() throws SQLException {
    return false;
  }

  @Override @NotSupported
  public void setCursorName( String arg0 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Named cursors not supported" );
  }

  @Override
  public void setEscapeProcessing( boolean arg0 ) throws SQLException {
    // ignored
  }

  @Override
  public void setFetchDirection( int direction ) throws SQLException {
    if ( direction != ResultSet.FETCH_FORWARD ) {
      throw new SQLFeatureNotSupportedException( "Only FETCH_FORWARD direction is supported" );
    }
  }

  @Override
  public void setFetchSize( int arg0 ) throws SQLException {
    // ignored
  }

  @Override
  public void setMaxFieldSize( int arg0 ) throws SQLException {
    // ignored
  }

  @Override
  public void setPoolable( boolean arg0 ) throws SQLException {
    // ignored
  }

  @Override
  public void setQueryTimeout( int arg0 ) throws SQLException {
  }

  /**
   * @return the maxRows
   */
  @Override
  public int getMaxRows() {
    return maxRows;
  }

  /**
   * @param maxRows
   *          the maxRows to set
   */
  @Override
  public void setMaxRows( int maxRows ) {
    this.maxRows = maxRows;
  }

  public void closeOnCompletion() throws SQLException {
  }

  public boolean isCloseOnCompletion() throws SQLException {
    return false;
  }

  @Override
  public void executePushQuery( String sql, IStreamingParams streamParams, Observer<List<RowMetaAndData>> consumer )
    throws Exception {
    if ( !connection.isLocal() ) {
      throw new UnsupportedOperationException( "Only available in local mode." );
    }
    connection.getClientService().query( sql, streamParams, connection.getParameters(), consumer );
  }

  @Override
  public boolean isWrapperFor( Class<?> type ) throws SQLException {
    if ( type.isAssignableFrom( ThinStatement.class ) || type.isAssignableFrom( IDataServiceClientService.class ) ) {
      return true;
    }
    return super.isWrapperFor( type );
  }

  @Override
  public <T> T unwrap( Class<T> type ) throws SQLException {
    if ( type.isAssignableFrom( ThinStatement.class ) ) {
      return type.cast( this );
    }
    if ( type.isAssignableFrom( IDataServiceClientService.class ) ) {
      return type.cast( connection.getClientService() );
    }
    return super.unwrap( type );
  }
}
