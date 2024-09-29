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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import org.pentaho.di.core.row.RowMetaInterface;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

public class RowsResultSet extends BaseResultSet implements ResultSet {
  private ImmutableList<Object[]> rows;

  public RowsResultSet( RowMetaInterface rowMeta, List<Object[]> rows ) {
    super( rowMeta );
    this.rows = ImmutableList.copyOf( rows );
  }

  @Override protected Object[] retrieveRow( int i ) throws Exception {
    return Range.openClosed( 0, rows.size() ).contains( i ) ? rows.get( i - 1 ) : null;
  }

  @Override public int size() throws SQLException {
    return rows.size();
  }

  @Override
  public void close() throws SQLException {
    rows = null;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return rows == null;
  }

  @Override
  public String getCursorName() throws SQLException {
    return "rows";
  }

  @Override
  public int getFetchDirection() throws SQLException {
    return ResultSet.FETCH_UNKNOWN;
  }

  @Override
  public int getFetchSize() throws SQLException {
    return 0;
  }

  @Override
  public int getHoldability() throws SQLException {
    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return new ThinResultSetMetaData( "rows", getRowMeta() );
  }

  @Override
  public int getType() throws SQLException {
    return ResultSet.TYPE_SCROLL_INSENSITIVE;
  }

  @Override
  public void setFetchDirection( int direction ) throws SQLException {
  }

  @Override
  public void setFetchSize( int rows ) throws SQLException {
  }
}
