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

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;

import java.math.BigDecimal;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class ThinResultSetMetaData extends ThinBase implements ResultSetMetaData {

  private String serviceName;
  private RowMetaInterface rowMeta;

  public ThinResultSetMetaData( String serviceName, RowMetaInterface rowMeta ) {
    this.serviceName = serviceName;
    this.rowMeta = rowMeta;
  }

  @Override
  public String getCatalogName( int column ) throws SQLException {
    return null;
  }

  @Override
  public String getColumnClassName( int column ) throws SQLException {
    switch ( rowMeta.getValueMeta( column - 1 ).getType() ) {
      case ValueMetaInterface.TYPE_STRING:
        return java.lang.String.class.getName();
      case ValueMetaInterface.TYPE_NUMBER:
        return java.lang.Double.class.getName();
      case ValueMetaInterface.TYPE_DATE:
        return java.util.Date.class.getName();
      case ValueMetaInterface.TYPE_BIGNUMBER:
        return BigDecimal.class.getName();
      case ValueMetaInterface.TYPE_INTEGER:
        return java.lang.Long.class.getName();
      case ValueMetaInterface.TYPE_BOOLEAN:
        return java.lang.Boolean.class.getName();
      case ValueMetaInterface.TYPE_BINARY:
        return ( new byte[0] ).getClass().getName();
      default:
        throw new SQLException( "Unknown data type for column " + column );
    }
  }

  @Override
  public int getColumnCount() throws SQLException {
    return rowMeta.size();
  }

  @Override
  public int getColumnDisplaySize( int column ) throws SQLException {
    return rowMeta.getValueMeta( column - 1 ).getLength();
  }

  @Override
  public String getColumnLabel( int column ) throws SQLException {
    return rowMeta.getValueMeta( column - 1 ).getName();
  }

  @Override
  public String getColumnName( int column ) throws SQLException {
    return rowMeta.getValueMeta( column - 1 ).getName();
  }

  @Override
  public int getColumnType( int column ) throws SQLException {
    switch ( rowMeta.getValueMeta( column - 1 ).getType() ) {
      case ValueMetaInterface.TYPE_STRING:
        return java.sql.Types.VARCHAR;
      case ValueMetaInterface.TYPE_NUMBER:
        return java.sql.Types.DOUBLE;
      case ValueMetaInterface.TYPE_TIMESTAMP:
        return java.sql.Types.TIMESTAMP;
      case ValueMetaInterface.TYPE_DATE:
        return java.sql.Types.TIMESTAMP;
      case ValueMetaInterface.TYPE_BIGNUMBER:
        return java.sql.Types.DECIMAL;
      case ValueMetaInterface.TYPE_INTEGER:
        return java.sql.Types.BIGINT;
      case ValueMetaInterface.TYPE_BOOLEAN:
        return java.sql.Types.BIT;
      case ValueMetaInterface.TYPE_BINARY:
        return java.sql.Types.BLOB;
      case ValueMetaInterface.TYPE_INET:
        return java.sql.Types.BINARY;
      default:
        throw new SQLException( "Unknown data type for column " + column );
    }
  }

  @Override
  public String getColumnTypeName( int column ) throws SQLException {
    return rowMeta.getValueMeta( column - 1 ).getTypeDesc();
  }

  @Override
  public int getPrecision( int column ) throws SQLException {
    return rowMeta.getValueMeta( column - 1 ).getLength();
  }

  @Override
  public int getScale( int column ) throws SQLException {
    return rowMeta.getValueMeta( column - 1 ).getPrecision();
  }

  @Override
  public String getSchemaName( int column ) throws SQLException {
    return null;
  }

  @Override
  public String getTableName( int column ) throws SQLException {
    return serviceName;
  }

  @Override
  public boolean isAutoIncrement( int column ) throws SQLException {
    return false;
  }

  @Override
  public boolean isCaseSensitive( int column ) throws SQLException {
    return rowMeta.getValueMeta( column - 1 ).isCaseInsensitive();
  }

  @Override
  public boolean isCurrency( int column ) throws SQLException {
    return false;
  }

  @Override
  public boolean isDefinitelyWritable( int column ) throws SQLException {
    return false;
  }

  @Override
  public int isNullable( int column ) throws SQLException {
    return columnNullableUnknown;
  }

  @Override
  public boolean isReadOnly( int column ) throws SQLException {
    return true;
  }

  @Override
  public boolean isSearchable( int column ) throws SQLException {
    return false;
  }

  @Override
  public boolean isSigned( int column ) throws SQLException {
    return rowMeta.getValueMeta( column - 1 ).isNumeric();
  }

  @Override
  public boolean isWritable( int column ) throws SQLException {
    return false;
  }
}
