/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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
