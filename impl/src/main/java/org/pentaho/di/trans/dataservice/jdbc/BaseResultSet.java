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

import com.google.common.base.Throwables;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.dataservice.jdbc.annotation.NotSupported;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

/**
 * @author nhudak
 */
public abstract class BaseResultSet extends ThinBase implements ResultSet {
  private final RowMetaInterface rowMeta;
  private Object[] currentRow;
  private int rowNumber = 0;
  private boolean lastNull;
  private ThinStatement statement;

  public BaseResultSet( RowMetaInterface rowMeta ) {
    this.rowMeta = rowMeta;
  }

  protected abstract Object[] retrieveRow( int i ) throws Exception;

  protected abstract int size() throws SQLException;

  @Override @NotSupported
  public boolean rowDeleted() throws SQLException {
    throw new SQLFeatureNotSupportedException( "Result set is read-only" );
  }

  @Override @NotSupported
  public boolean rowInserted() throws SQLException {
    throw new SQLFeatureNotSupportedException( "Result set is read-only" );
  }

  @Override @NotSupported
  public boolean rowUpdated() throws SQLException {
    throw new SQLFeatureNotSupportedException( "Result set is read-only" );
  }

  @Override
  public boolean wasNull() throws SQLException {
    return lastNull;
  }

  @Override
  public Date getDate( int index ) throws SQLException {
    return getDate( index, Calendar.getInstance() );
  }

  @Override
  public Date getDate( String columnName ) throws SQLException {
    return getDate( columnName, Calendar.getInstance() );
  }

  @Override
  public Date getDate( int index, Calendar calendar ) throws SQLException {
    Date date = null;
    if ( setCalendar( index, calendar ) ) {
      date = new Date( calendar.getTimeInMillis() );
    }
    return date;
  }

  private Boolean setCalendar( int index, final Calendar calendar ) throws SQLException {
    return getValue( index, new ValueRetriever<Boolean>() {
      @Override public Boolean value( int index ) throws Exception {
        java.util.Date date = rowMeta.getDate( currentRow, index );
        if ( date != null ) {
          calendar.setTime( date );
          return true;
        } else {
          return false;
        }
      }
    } );
  }

  @Override
  public Date getDate( String columnName, Calendar calendar ) throws SQLException {
    return getDate( findColumn( columnName ), calendar );
  }

  @Override
  public double getDouble( int index ) throws SQLException {
    return getNonNullableValue( index, new ValueRetriever<Double>() {
      @Override public Double value( int index ) throws Exception {
        return rowMeta.getNumber( currentRow, index );
      }
    }, 0.0 );
  }

  @Override
  public double getDouble( String columnName ) throws SQLException {
    return getDouble( findColumn( columnName ) );
  }

  @Override @NotSupported
  public Array getArray( int arg0 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Arrays are not supported" );
  }

  @Override @NotSupported
  public Array getArray( String arg0 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Arrays are not supported" );
  }

  @Override @NotSupported
  public InputStream getAsciiStream( int arg0 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "ASCII streams are not supported" );
  }

  @Override @NotSupported
  public InputStream getAsciiStream( String arg0 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "ASCII streams are not supported" );
  }

  @Override
  public BigDecimal getBigDecimal( int index ) throws SQLException {
    return getValue( index, new ValueRetriever<BigDecimal>() {
      @Override public BigDecimal value( int index ) throws Exception {
        return rowMeta.getBigNumber( currentRow, index );
      }
    } );
  }

  @Override
  public BigDecimal getBigDecimal( String columnName ) throws SQLException {
    return getBigDecimal( findColumn( columnName ) );
  }

  @Override
  @Deprecated
  public BigDecimal getBigDecimal( int index, int arg1 ) throws SQLException {
    return getBigDecimal( index );
  }

  @Override
  @Deprecated
  public BigDecimal getBigDecimal( String columnName, int arg1 ) throws SQLException {
    return getBigDecimal( findColumn( columnName ) );
  }

  @Override @NotSupported
  public InputStream getBinaryStream( int arg0 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Binary streams are not supported" );
  }

  @Override @NotSupported
  public InputStream getBinaryStream( String arg0 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Binary streams are not supported" );
  }

  @Override @NotSupported
  public Blob getBlob( int index ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "BLOBs are not supported" );
  }

  @Override @NotSupported
  public Blob getBlob( String arg0 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "BLOBs are not supported" );
  }

  @Override
  public boolean getBoolean( int index ) throws SQLException {
    return getNonNullableValue( index, new ValueRetriever<Boolean>() {
      @Override public Boolean value( int index ) throws Exception {
        return rowMeta.getBoolean( currentRow, index );
      }
    }, false );
  }

  @Override
  public boolean getBoolean( String columnName ) throws SQLException {
    return getBoolean( findColumn( columnName ) );
  }

  @Override
  public byte getByte( int index ) throws SQLException {
    long l = getLong( index );
    return (byte) l;
  }

  @Override
  public byte getByte( String columnName ) throws SQLException {
    return getByte( findColumn( columnName ) );
  }

  @Override
  public byte[] getBytes( int index ) throws SQLException {
    return getValue( index, new ValueRetriever<byte[]>() {
      @Override public byte[] value( int index ) throws Exception {
        return rowMeta.getBinary( currentRow, index );
      }
    } );
  }

  @Override
  public byte[] getBytes( String columnName ) throws SQLException {
    return getBytes( findColumn( columnName ) );
  }

  @Override @NotSupported
  public Reader getCharacterStream( int arg0 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Character streams are not supported" );
  }

  @Override @NotSupported
  public Reader getCharacterStream( String arg0 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Character streams are not supported" );
  }

  @Override @NotSupported
  public Clob getClob( int arg0 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "CLOBs are not supported" );
  }

  @Override @NotSupported
  public Clob getClob( String arg0 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "CLOBs are not supported" );
  }

  @Override
  public float getFloat( int index ) throws SQLException {
    return (float) getDouble( index );
  }

  @Override
  public float getFloat( String columnName ) throws SQLException {
    return getFloat( findColumn( columnName ) );
  }

  @Override
  public int getInt( int index ) throws SQLException {
    return (int) getLong( index );
  }

  @Override
  public int getInt( String columnName ) throws SQLException {
    return getInt( findColumn( columnName ) );
  }

  @Override
  public long getLong( int index ) throws SQLException {
    return getNonNullableValue( index, new ValueRetriever<Long>() {
      @Override public Long value( int index ) throws Exception {
        return rowMeta.getInteger( currentRow, index );
      }
    }, 0l );
  }

  @Override
  public long getLong( String columnName ) throws SQLException {
    return getLong( findColumn( columnName ) );
  }

  @Override @NotSupported
  public Reader getNCharacterStream( int arg0 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "NCharacter streams are not supported" );
  }

  @Override @NotSupported
  public Reader getNCharacterStream( String arg0 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "NCharacter streams are not supported" );
  }

  @Override @NotSupported
  public NClob getNClob( int arg0 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "NCLOBs are not supported" );
  }

  @Override @NotSupported
  public NClob getNClob( String arg0 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "NCLOBs are not supported" );
  }

  @Override @NotSupported
  public String getNString( int arg0 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "NStrings are not supported" );
  }

  @Override @NotSupported
  public String getNString( String arg0 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "NStrings are not supported" );
  }

  @Override
  public Object getObject( final int index ) throws SQLException {
    return getValue( index, new ValueRetriever<Object>() {
      @Override public Object value( int i ) throws Exception {
        if ( getRowMeta().getValueMeta( i ).getType() == ValueMetaInterface.TYPE_DATE ) {
          return getTimestamp( index );
        }
        return currentRow[ i ];
      }
    } );
  }

  @Override
  public Object getObject( String columnName ) throws SQLException {
    return getObject( findColumn( columnName ) );
  }

  @Override
  public Object getObject( int index, Map<String, Class<?>> arg1 ) throws SQLException {
    return getObject( index );
  }

  @Override
  public Object getObject( String columnName, Map<String, Class<?>> arg1 ) throws SQLException {
    return getObject( columnName );
  }

  @Override @NotSupported
  public Ref getRef( int arg0 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Refs are not supported" );
  }

  @Override @NotSupported
  public Ref getRef( String arg0 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Refs are not supported" );
  }

  @Override @NotSupported
  public RowId getRowId( int arg0 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "RowIDs are not supported" );
  }

  @Override @NotSupported
  public RowId getRowId( String arg0 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "RowIDs are not supported" );
  }

  @Override @NotSupported
  public SQLXML getSQLXML( int arg0 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "SQLXML is not supported" );
  }

  @Override @NotSupported
  public SQLXML getSQLXML( String arg0 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "SQLXML is not supported" );
  }

  @Override
  public short getShort( int index ) throws SQLException {
    long l = getLong( index );
    return (short) l;
  }

  @Override
  public short getShort( String columnName ) throws SQLException {
    return getShort( findColumn( columnName ) );
  }

  @Override
  public String getString( int index ) throws SQLException {
    return getValue( index, new ValueRetriever<String>() {
      @Override public String value( int index ) throws Exception {
        return rowMeta.getString( currentRow, index );
      }
    } );
  }

  @Override
  public String getString( String columnName ) throws SQLException {
    return getString( findColumn( columnName ) );
  }

  @Override
  public Time getTime( int index ) throws SQLException {
    return getTime( index, Calendar.getInstance() );
  }

  @Override
  public Time getTime( String columnLabel ) throws SQLException {
    return getTime( findColumn( columnLabel ), Calendar.getInstance() );
  }

  @Override
  public Time getTime( int index, Calendar calendar ) throws SQLException {
    Time time = null;
    if ( setCalendar( index, calendar ) ) {
      time = new Time( calendar.getTimeInMillis() );
    }
    return time;
  }

  @Override
  public Time getTime( String columnLabel, Calendar calendar ) throws SQLException {
    return getTime( findColumn( columnLabel ), calendar );
  }

  @Override
  public Timestamp getTimestamp( int index ) throws SQLException {
    return getTimestamp( index, Calendar.getInstance() );
  }

  @Override
  public Timestamp getTimestamp( String columnName ) throws SQLException {
    return getTimestamp( findColumn( columnName ), Calendar.getInstance() );
  }

  @Override
  public Timestamp getTimestamp( int index, Calendar calendar ) throws SQLException {
    Timestamp timestamp = null;
    if ( setCalendar( index, calendar ) ) {
      timestamp = new Timestamp( calendar.getTimeInMillis() );
    }
    return timestamp;
  }

  @Override
  public Timestamp getTimestamp( String columnName, Calendar calendar ) throws SQLException {
    return getTimestamp( findColumn( columnName ), calendar );
  }

  @Override @NotSupported
  public URL getURL( int arg0 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "URLs are not supported" );
  }

  @Override @NotSupported
  public URL getURL( String arg0 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "URLs are not supported" );
  }

  @Override @NotSupported
  @Deprecated
  public InputStream getUnicodeStream( int arg0 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Unicode streams are not supported" );
  }

  @Override @NotSupported
  @Deprecated
  public InputStream getUnicodeStream( String arg0 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Unicode streams are not supported" );
  }

  @Override @NotSupported
  public void updateArray( int arg0, Array arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateArray( String arg0, Array arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateAsciiStream( int arg0, InputStream arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateAsciiStream( String arg0, InputStream arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateAsciiStream( int arg0, InputStream arg1, int arg2 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateAsciiStream( String arg0, InputStream arg1, int arg2 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateAsciiStream( int arg0, InputStream arg1, long arg2 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateAsciiStream( String arg0, InputStream arg1, long arg2 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateBigDecimal( int arg0, BigDecimal arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateBigDecimal( String arg0, BigDecimal arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateBinaryStream( int arg0, InputStream arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateBinaryStream( String arg0, InputStream arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateBinaryStream( int arg0, InputStream arg1, int arg2 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateBinaryStream( String arg0, InputStream arg1, int arg2 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateBinaryStream( int arg0, InputStream arg1, long arg2 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateBinaryStream( String arg0, InputStream arg1, long arg2 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateBlob( int arg0, Blob arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateBlob( String arg0, Blob arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateBlob( int arg0, InputStream arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateBlob( String arg0, InputStream arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateBlob( int arg0, InputStream arg1, long arg2 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateBlob( String arg0, InputStream arg1, long arg2 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateBoolean( int arg0, boolean arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateBoolean( String arg0, boolean arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateByte( int arg0, byte arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateByte( String arg0, byte arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateBytes( int arg0, byte[] arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateBytes( String arg0, byte[] arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateCharacterStream( int arg0, Reader arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateCharacterStream( String arg0, Reader arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateCharacterStream( int arg0, Reader arg1, int arg2 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateCharacterStream( String arg0, Reader arg1, int arg2 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateCharacterStream( int arg0, Reader arg1, long arg2 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateCharacterStream( String arg0, Reader arg1, long arg2 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateClob( int arg0, Clob arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateClob( String arg0, Clob arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateClob( int arg0, Reader arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateClob( String arg0, Reader arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateClob( int arg0, Reader arg1, long arg2 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateClob( String arg0, Reader arg1, long arg2 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateDate( int arg0, Date arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateDate( String arg0, Date arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateDouble( int arg0, double arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateDouble( String arg0, double arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateFloat( int arg0, float arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateFloat( String arg0, float arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateInt( int arg0, int arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateInt( String arg0, int arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateLong( int arg0, long arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateLong( String arg0, long arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateNCharacterStream( int arg0, Reader arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateNCharacterStream( String arg0, Reader arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateNCharacterStream( int arg0, Reader arg1, long arg2 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateNCharacterStream( String arg0, Reader arg1, long arg2 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateNClob( int arg0, NClob arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateNClob( String arg0, NClob arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateNClob( int arg0, Reader arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateNClob( String arg0, Reader arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateNClob( int arg0, Reader arg1, long arg2 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateNClob( String arg0, Reader arg1, long arg2 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateNString( int arg0, String arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateNString( String arg0, String arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateNull( int arg0 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateNull( String arg0 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateObject( int arg0, Object arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateObject( String arg0, Object arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateObject( int arg0, Object arg1, int arg2 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateObject( String arg0, Object arg1, int arg2 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateRef( int arg0, Ref arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateRef( String arg0, Ref arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateRow() throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateRowId( int arg0, RowId arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateRowId( String arg0, RowId arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateSQLXML( int arg0, SQLXML arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateSQLXML( String arg0, SQLXML arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateShort( int arg0, short arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateShort( String arg0, short arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateString( int arg0, String arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateString( String arg0, String arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateTime( int arg0, Time arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateTime( String arg0, Time arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateTimestamp( int arg0, Timestamp arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateTimestamp( String arg0, Timestamp arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  public <T> T getObject( int columnIndex, Class<T> type ) throws SQLException {
    return type.cast( getObject( columnIndex ) );
  }

  public <T> T getObject( String columnLabel, Class<T> type ) throws SQLException {
    return type.cast( getObject( columnLabel ) );
  }

  private <T> T getValue( int index, ValueRetriever<T> valueRetriever ) throws SQLException {
    if ( index < 1 || rowMeta.size() < index ) {
      throw new SQLException( "Invalid column reference: " + index );
    }

    if ( currentRow == null ) {
      throw new SQLException( "Current row is not selected" );
    }

    try {
      T value = valueRetriever.value( index - 1 );
      lastNull = value == null;
      return value;
    } catch ( Exception e ) {
      Throwables.propagateIfPossible( e, SQLException.class );
      throw new SQLException( e );
    }
  }

  /**
   * Retrieves the value of a non-nullable column.  If the column has a SQL NULL value,
   * return the specified defaultValue instead.
   * Methods which return primitive values (getLong, getInt, getBoolean, etc.) cannot
   * return null.  The client can determine whether the value was SQL NULL or actually
   * equal to defaultValue by using the .wasNull() method.
   * @throws SQLException
   */
  private <T> T getNonNullableValue( int index, ValueRetriever<T> valueRetriever, T defaultValue )
          throws SQLException {
    T value = getValue( index, valueRetriever );
    return value == null ? defaultValue : value;
  }

  @Override
  public int getConcurrency() throws SQLException {
    return ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public int getRow() throws SQLException {
    return currentRow == null ? 0 : rowNumber;
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    return verifyOpen() && rowNumber > size() || size() == 0;
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    return verifyOpen() && rowNumber == 0;
  }

  @Override
  public boolean isFirst() throws SQLException {
    return verifyOpen() && currentRow != null && rowNumber == 1;
  }

  @Override
  public boolean isLast() throws SQLException {
    return verifyOpen() && currentRow != null && rowNumber == size();
  }

  @Override
  public int findColumn( String column ) throws SQLException {
    int i = rowMeta.indexOfValue( column );
    if ( i < 0 ) {
      throw new SQLException( "Invalid column reference: " + column );
    }
    return i + 1;
  }

  public RowMetaInterface getRowMeta() {
    return rowMeta;
  }

  @Override @NotSupported
  public void insertRow() throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updating result sets are not supported" );
  }

  @Override
  public boolean next() throws SQLException {
    return !isClosed() && relative( 1 );
  }

  @Override public boolean absolute( int row ) throws SQLException {
    verifyOpen();
    while ( row < 0 ) {
      row += size();
    }
    try {
      currentRow = retrieveRow( row );
      rowNumber = currentRow != null ? row : row > size() ? size() + 1 : 0;
      return currentRow != null;
    } catch ( Exception e ) {
      Throwables.propagateIfPossible( e, SQLException.class );
      throw new SQLException( e );
    }
  }

  @Override @NotSupported
  public void cancelRowUpdates() throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override
  public void afterLast() throws SQLException {
    absolute( size() + 1 );
  }

  @Override
  public void beforeFirst() throws SQLException {
    absolute( 0 );
  }

  protected boolean verifyOpen() throws SQLException {
    if ( isClosed() ) {
      throw new SQLException( "Result set is closed" );
    }
    return true;
  }

  @Override @NotSupported
  public void deleteRow() throws SQLException {
    throw new SQLFeatureNotSupportedException( "Deletes are not supported" );
  }

  @Override
  public boolean first() throws SQLException {
    return absolute( 1 );
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
  }

  @Override @NotSupported
  public void moveToInsertRow() throws SQLException {
    throw new SQLFeatureNotSupportedException( "Inserts are not supported" );
  }

  @Override
  public boolean last() throws SQLException {
    return absolute( -1 );
  }

  protected void setStatement( ThinStatement statement ) {
    this.statement = statement;
  }

  @Override
  public Statement getStatement() throws SQLException {
    return statement;
  }

  @Override
  public boolean previous() throws SQLException {
    return relative( -1 );
  }

  @Override
  public void refreshRow() throws SQLException {
  }

  @Override
  public boolean relative( int rows ) throws SQLException {
    return absolute( rowNumber + rows );
  }

  public Object[] getCurrentRow() {
    return currentRow;
  }

  private interface ValueRetriever<T> {
    T value( int index ) throws Exception;
  }
}
