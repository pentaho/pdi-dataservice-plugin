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

import org.pentaho.di.core.exception.KettleEOFException;
import org.pentaho.di.core.exception.KettleFileException;

import java.io.DataInputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ThinResultSet extends BaseResultSet {

  private final ThinResultHeader thinResultHeader;
  private final AtomicBoolean stopped = new AtomicBoolean( false );
  private DataInputStream dataInputStream;
  private int size = 1;

  public ThinResultSet( ThinResultHeader header, DataInputStream dataInputStream ) {
    super( header.getRowMeta() );
    this.thinResultHeader = header;
    this.dataInputStream = dataInputStream;
  }

  @Override
  public void close() {
    try {
      // Kill the service transformation on the server...
      // Only ever try once.
      //
      if ( dataInputStream != null ) {
        dataInputStream.close();
      }
      if ( stopped.compareAndSet( false, true ) ) {
        // TODO issue stop to serviceTrans: http://jira.pentaho.com/browse/BACKLOG-4594
        ThinDriver.logger.warning( "Need to stop " + thinResultHeader.getServiceTransName() );
      }
    } catch ( Exception e ) {
      ThinDriver.logger.warning( e.getMessage() );
    } finally {
      dataInputStream = null;
    }
  }

  @Override
  public String getCursorName() throws SQLException {
    return thinResultHeader.getServiceName();
  }

  @Override
  public int getType() throws SQLException {
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public int getFetchDirection() throws SQLException {
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public void setFetchDirection( int direction ) throws SQLException {
    if ( direction != FETCH_FORWARD ) {
      throw new SQLFeatureNotSupportedException( "Only FETCH_FORWARD is allowed" );
    }
  }

  @Override
  public int getFetchSize() throws SQLException {
    return 0;
  }

  @Override
  public void setFetchSize( int direction ) throws SQLException {
  }

  @Override
  public int getHoldability() throws SQLException {
    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return new ThinResultSetMetaData( thinResultHeader.getServiceName(), getRowMeta() );
  }

  public ThinResultHeader getHeader() {
    return thinResultHeader;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return dataInputStream == null;
  }

  @Override protected Object[] retrieveRow( int i ) throws Exception {
    if ( isAfterLast() ? i > size() : i == getRow() ) {
      return getCurrentRow();
    } else if ( i == getRow() + 1 ) {
      return readData();
    } else {
      throw new SQLFeatureNotSupportedException( "Scrollable result sets are not supported" );
    }
  }

  private Object[] readData() throws KettleFileException, SQLException, IOException {
    try {
      Object[] data = getRowMeta().readData( dataInputStream );
      size += 1;
      return data;
    } catch ( KettleEOFException e ) {
      size = getRow();
      dataInputStream.close();
      return null;
    }
  }

  @Override
  protected int size() throws SQLException {
    return size;
  }

}
