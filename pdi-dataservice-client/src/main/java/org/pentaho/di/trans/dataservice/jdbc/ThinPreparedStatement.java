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

import com.google.common.base.Throwables;
import org.pentaho.di.core.exception.KettleSQLException;
import org.pentaho.di.core.jdbc.ThinUtil;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBigNumber;
import org.pentaho.di.core.row.value.ValueMetaBoolean;
import org.pentaho.di.core.row.value.ValueMetaDate;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaNumber;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;
import org.pentaho.di.trans.dataservice.jdbc.annotation.NotSupported;
import org.pentaho.di.trans.dataservice.jdbc.api.IThinPreparedStatement;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class ThinPreparedStatement extends ThinStatement implements IThinPreparedStatement {

  public static final SimpleDateFormat FORMAT = new SimpleDateFormat( "'['yyyy/MM/dd HH:mm:ss.SSS']'" );
  private final String sql; // contains ? placeholders

  protected List<Integer> placeholderIndexes;
  protected ValueMetaInterface[] paramMeta;
  protected Object[] paramData;

  public ThinPreparedStatement( ThinConnection connection, String sql ) throws SQLException {
    this( connection, sql, new ThinResultFactory() );
  }

  public ThinPreparedStatement( ThinConnection connection, String sql, ThinResultFactory resultFactory ) throws SQLException {
    super( connection, resultFactory );
    this.sql = sql;

    analyzeSql();
  }

  public void analyzeSql() throws SQLException {
    try {
      placeholderIndexes = new ArrayList<Integer>();

      int index = 0;
      while ( index < sql.length() ) {
        index = ThinUtil.skipChars( sql, index, '\'', '"' );
        if ( index < sql.length() ) {
          if ( sql.charAt( index ) == '?' ) {
            // placeholder found.
            placeholderIndexes.add( index );
          }
        }

        index++;
      }
      paramData = new Object[placeholderIndexes.size()];
      paramMeta = new ValueMetaInterface[placeholderIndexes.size()];
      // Null Strings is the default.
      for ( int i = 0; i < placeholderIndexes.size(); i++ ) {
        paramMeta[i] = new ValueMetaString( "param-" + ( i + 1 ) );
      }

    } catch ( Exception e ) {
      Throwables.propagateIfPossible( e, SQLException.class );
      throw new SQLException( e );
    }
  }

  public String replaceSql() throws SQLException {
    try {
      StringBuilder newSql = new StringBuilder( sql );

      for ( int i = placeholderIndexes.size() - 1; i >= 0; i-- ) {
        int index = placeholderIndexes.get( i );
        ValueMetaInterface valueMeta = paramMeta[i];
        if ( valueMeta == null ) {
          throw new SQLException( "Parameter " + ( i + 1 ) + " was not specified" );
        }
        String replacement = null;
        if ( valueMeta.isNull( paramData[i] ) ) {
          replacement = "NULL";
        } else {
          switch ( valueMeta.getType() ) {
            case ValueMetaInterface.TYPE_STRING:
              replacement = "'" + valueMeta.getString( paramData[i] ) + "'";
              break;
            case ValueMetaInterface.TYPE_NUMBER:
              double d = valueMeta.getNumber( paramData[i] );
              replacement = Double.toString( d );
              break;
            case ValueMetaInterface.TYPE_INTEGER:
              long l = valueMeta.getInteger( paramData[i] );
              replacement = Long.toString( l );
              break;
            case ValueMetaInterface.TYPE_DATE:
              java.util.Date date = valueMeta.getDate( paramData[i] );
              replacement = FORMAT.format( date );
              break;
            case ValueMetaInterface.TYPE_BIGNUMBER:
              BigDecimal bd = valueMeta.getBigNumber( paramData[i] );
              replacement = bd.toString();
              break;
            case ValueMetaInterface.TYPE_BOOLEAN:
              boolean b = valueMeta.getBoolean( paramData[i] );
              replacement = b ? "TRUE" : "FALSE";
              break;
            default:
              break;
          }
        }
        if ( replacement == null ) {
          throw new KettleSQLException( "Unhandled data type: "
            + valueMeta.getTypeDesc() + " replacing parameter " + ( i + 1 ) );
        }

        // replace the ?
        //
        newSql.replace( index, index + 1, replacement );
      }

      return newSql.toString();
    } catch ( Exception e ) {
      throw new SQLException( "Unexpected enhancing SQL to include specified parameters", e );
    }
  }

  @Override @NotSupported
  public void addBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException( "Batch operations are not supported" );
  }

  @Override
  public void clearParameters() throws SQLException {
    analyzeSql();
  }

  @Override
  public boolean execute() throws SQLException {
    return execute( replaceSql() );
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    return executeQuery( replaceSql() );
  }

  @Override
  public ResultSet executeQuery( IDataServiceClientService.StreamingMode windowMode,
                                 long windowSize, long windowEvery,
                                 long windowLimit ) throws SQLException {
    return executeQuery( replaceSql(), windowMode, windowSize, windowEvery, windowLimit );
  }

  @Override @NotSupported
  public int executeUpdate() throws SQLException {
    throw new SQLFeatureNotSupportedException( "Update operations are not supported" );
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    ResultSet resultSet = getResultSet();
    return resultSet == null ? null : resultSet.getMetaData();
  }

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException {
    return new ThinParameterMetaData( this );
  }

  @Override @NotSupported
  public void setArray( int nr, Array value ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Arrays are not supported" );
  }

  @Override @NotSupported
  public void setAsciiStream( int nr, InputStream value ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "ASCII Streams are not supported" );
  }

  @Override @NotSupported
  public void setAsciiStream( int nr, InputStream value, int arg2 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "ASCII Streams are not supported" );
  }

  @Override @NotSupported
  public void setAsciiStream( int nr, InputStream value, long arg2 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "ASCII Streams are not supported" );
  }

  @Override
  public void setBigDecimal( int nr, BigDecimal value ) throws SQLException {
    setValue( nr, value, new ValueMetaBigNumber( "param-" + nr ) );
  }

  @Override @NotSupported
  public void setBinaryStream( int nr, InputStream value ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Binary Streams are not supported" );
  }

  @Override @NotSupported
  public void setBinaryStream( int nr, InputStream value, int arg2 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Binary Streams are not supported" );
  }

  @Override @NotSupported
  public void setBinaryStream( int nr, InputStream value, long arg2 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Binary Streams are not supported" );
  }

  @Override @NotSupported
  public void setBlob( int nr, Blob value ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "BLOB parameters are not supported" );
  }

  @Override @NotSupported
  public void setBlob( int nr, InputStream value ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "BLOB parameters are not supported" );
  }

  @Override @NotSupported
  public void setBlob( int nr, InputStream value, long arg2 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "BLOB parameters are not supported" );
  }

  @Override
  public void setBoolean( int nr, boolean value ) throws SQLException {
    setValue( nr, value, new ValueMetaBoolean( "param-" + nr ) );
  }

  @Override
  public void setByte( int nr, byte value ) throws SQLException {
    setValue( nr, (long) value, new ValueMetaInteger( "param-" + nr ) );
  }

  @Override @NotSupported
  public void setBytes( int nr, byte[] value ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Binary parameters are not supported" );
  }

  @Override @NotSupported
  public void setCharacterStream( int nr, Reader value ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Character stream parameters are not supported" );
  }

  @Override @NotSupported
  public void setCharacterStream( int nr, Reader value, int arg2 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Character stream parameters are not supported" );
  }

  @Override @NotSupported
  public void setCharacterStream( int nr, Reader value, long arg2 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Character stream parameters are not supported" );
  }

  @Override @NotSupported
  public void setClob( int nr, Clob value ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "CLOB parameters are not supported" );
  }

  @Override @NotSupported
  public void setClob( int nr, Reader value ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "CLOB parameters are not supported" );
  }

  @Override @NotSupported
  public void setClob( int nr, Reader value, long arg2 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "CLOB parameters are not supported" );
  }

  @Override
  public void setDate( int nr, java.sql.Date value ) throws SQLException {
    setDate( nr, value, Calendar.getInstance() );
  }

  @Override
  public void setDate( int nr, java.sql.Date value, Calendar calendar ) throws SQLException {
    setTimeValue( nr, value, calendar );
  }

  @Override
  public void setDouble( int nr, double value ) throws SQLException {
    setValue( nr, value, new ValueMetaNumber( "param-" + nr ) );
  }

  @Override
  public void setFloat( int nr, float value ) throws SQLException {
    setDouble( nr, value );
  }

  @Override
  public void setInt( int nr, int value ) throws SQLException {
    setValue( nr, (long) value, new ValueMetaInteger( "param-" + nr ) );
  }

  @Override
  public void setLong( int nr, long value ) throws SQLException {
    setValue( nr, value, new ValueMetaInteger( "param-" + nr ) );
  }

  @Override @NotSupported
  public void setNCharacterStream( int nr, Reader value ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "NCharacter stream parameters are not supported" );
  }

  @Override @NotSupported
  public void setNCharacterStream( int nr, Reader value, long arg2 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "NCharacter stream parameters are not supported" );
  }

  @Override @NotSupported
  public void setNClob( int nr, NClob value ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "NCLOB parameters are not supported" );
  }

  @Override @NotSupported
  public void setNClob( int nr, Reader value ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "NCLOB parameters are not supported" );
  }

  @Override @NotSupported
  public void setNClob( int nr, Reader value, long arg2 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "NCLOB parameters are not supported" );
  }

  @Override @NotSupported
  public void setNString( int nr, String value ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "NString parameters are not supported" );
  }

  @Override
  public void setNull( int nr, int sqlType ) throws SQLException {
    setValue( nr, null, ThinUtil.getValueMeta( "param-" + nr, sqlType ) );
  }

  @Override
  public void setNull( int nr, int value, String typeName ) throws SQLException {
    setNull( nr, value );
  }

  @Override
  public void setObject( int nr, Object value ) throws SQLException {
    if ( value == null ) {
      setNull( nr, Types.OTHER );
    } else if ( value instanceof String ) {
      setString( nr, (String) value );
    } else if ( value instanceof Long ) {
      setLong( nr, (Long) value );
    } else if ( value instanceof Integer ) {
      setInt( nr, (Integer) value );
    } else if ( value instanceof Byte ) {
      setByte( nr, (Byte) value );
    } else if ( value instanceof Date ) {
      setTimeValue( nr, (Date) value, Calendar.getInstance() );
    } else if ( value instanceof Boolean ) {
      setBoolean( nr, (Boolean) value );
    } else if ( value instanceof Double ) {
      setDouble( nr, (Double) value );
    } else if ( value instanceof Float ) {
      setFloat( nr, (Float) value );
    } else if ( value instanceof BigDecimal ) {
      setBigDecimal( nr, (BigDecimal) value );
    } else {
      throw new SQLException( "value of class " + value.getClass().getName() );
    }
  }

  @Override
  public void setObject( int nr, Object value, int targetSqlType ) throws SQLException {
    ThinDriver.logger.warning(
      String.format( "Ignoring targetSqlType (%s).  Deducing sql type from object (%s)",
        targetSqlType,
        value == null ? "NULL" : value.getClass().getName() ) );
    setObject( nr, value );
  }

  @Override
  public void setObject( int nr, Object value, int targetSqlType, int scaleOrLength ) throws SQLException {
    setObject( nr, value, targetSqlType );
  }

  @Override @NotSupported
  public void setRef( int nr, Ref value ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "REF parameters are not supported" );
  }

  @Override @NotSupported
  public void setRowId( int nr, RowId value ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "ROWID parameters are not supported" );
  }

  @Override @NotSupported
  public void setSQLXML( int nr, SQLXML value ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "SQLXML parameters are not supported" );
  }

  @Override
  public void setShort( int nr, short value ) throws SQLException {
    setLong( nr, Long.valueOf( value ) );
  }

  @Override
  public void setString( int nr, String value ) throws SQLException {
    setValue( nr, value, new ValueMetaString( "param-" + nr ) );
  }

  @Override
  public void setTime( int nr, Time value ) throws SQLException {
    setTime( nr, value, Calendar.getInstance() );
  }

  @Override
  public void setTime( int nr, Time value, Calendar calendar ) throws SQLException {
    setTimeValue( nr, value, calendar );
  }

  @Override
  public void setTimestamp( int nr, Timestamp value ) throws SQLException {
    setTimestamp( nr, value, Calendar.getInstance() );
  }

  @Override
  public void setTimestamp( int nr, Timestamp value, Calendar calendar ) throws SQLException {
    setTimeValue( nr, value, calendar );
  }

  @Override @NotSupported
  public void setURL( int nr, URL value ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "URL parameters are not supported" );
  }

  @Override @NotSupported
  @Deprecated
  public void setUnicodeStream( int nr, InputStream value, int arg2 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Unicode stream parameters are not supported" );
  }

  /**
   * @return the paramMeta
   */
  public ValueMetaInterface[] getParamMeta() {
    return paramMeta;
  }

  /**
   * @return the paramData
   */
  public Object[] getParamData() {
    return paramData;
  }

  protected void setValue( int nr, Object value, ValueMetaInterface valueMeta ) throws SQLException {
    if ( nr < 1 || nr > paramData.length ) {
      throw new SQLException( "parameterIndex does not correspond to a parameter marker in the SQL statement" );
    }
    paramData[nr - 1] = value;
    paramMeta[nr - 1] = valueMeta;
  }

  protected void setTimeValue( int nr, Date value, Calendar calendar ) throws SQLException {
    calendar.setTime( value );
    setValue( nr, calendar.getTime(), new ValueMetaDate( "param-" + nr ) );
  }

}
