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

import com.google.common.base.Throwables;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.KettleClientEnvironment;
import org.pentaho.di.core.exception.KettleEOFException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.dataservice.jdbc.annotation.NotSupported;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class ThinResultSet extends ThinBase implements ResultSet {

  private final ThinStatement statement;
  private final AtomicBoolean stopped = new AtomicBoolean( false );

  private DataInputStream dataInputStream;
  protected RowMetaInterface rowMeta;
  protected Object[] currentRow;
  private int rowNumber;
  private boolean lastNull;

  private String serviceName;

  private String serviceTransName;

  private String serviceObjectId;
  private String sqlTransName;
  private String sqlObjectId;

  protected ThinResultSet( ThinStatement statement ) {
    this.statement = statement;
  }

  protected ThinResultSet loadFromInputStream( DataInputStream dataInputStream ) throws IOException, KettleException {
    this.dataInputStream = dataInputStream;
    // Read the name of the service we're reading from
    //
    serviceName = dataInputStream.readUTF();

    // Get some information about what's going on on the slave server
    //
    serviceTransName = dataInputStream.readUTF();
    serviceObjectId = dataInputStream.readUTF();
    sqlTransName = dataInputStream.readUTF();
    sqlObjectId = dataInputStream.readUTF();

    // Get the row metadata...
    //
    if ( !KettleClientEnvironment.isInitialized() ) {
      KettleClientEnvironment.init();
    }
    rowMeta = new RowMeta( dataInputStream );

    return this;
  }

  static void stopService( RemoteClient remoteClient, String serviceObjectId, String serviceTransName ) throws Exception {
    final String stopTrans = "/stopTrans" + "/?name=" + URLEncoder.encode( serviceTransName, "UTF-8" ) + "&id="
      + Const.NVL( serviceObjectId, "" ) + "&xml=Y";
    String reply = remoteClient.execService( stopTrans );
  }

  public void cancel() throws SQLException {

    // Kill the service transformation on the server...
    // Only ever try once.
    //
    try {
      if ( dataInputStream != null ) {
        dataInputStream.close();
      }
      if ( stopped.compareAndSet( false, true ) ) {
        // TODO issue stop to serviceTrans: http://jira.pentaho.com/browse/BACKLOG-4594
        ThinDriver.logger.warning( "Need to stop " + serviceTransName );
      }
    } catch ( IOException e ) {
      throw new SQLException( e );
    } finally {
      dataInputStream = null;
      currentRow = null;
    }
  }

  @Override @NotSupported
  public boolean absolute( int rowNr ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Scrolleable resultsets are not supported" );
  }

  @Override @NotSupported
  public void afterLast() throws SQLException {
    throw new SQLFeatureNotSupportedException( "Scrollable resultsets are not supported" );
  }

  @Override @NotSupported
  public void beforeFirst() throws SQLException {
    throw new SQLFeatureNotSupportedException( "Scrollable resultsets are not supported" );
  }

  @Override @NotSupported
  public void cancelRowUpdates() throws SQLException {
    throw new SQLFeatureNotSupportedException( "Scrollable resultsets are not supported" );
  }

  @Override
  public void close() {
    try {
      cancel();
    } catch ( SQLException e ) {
      ThinDriver.logger.warning( e.getMessage() );
    }
  }

  @Override @NotSupported
  public void deleteRow() throws SQLException {
    throw new SQLFeatureNotSupportedException( "Scrollable resultsets are not supported" );
  }

  @Override
  public int findColumn( String column ) throws SQLException {
    int i = rowMeta.indexOfValue( column );
    if ( i < 0 ) {
      throw new SQLException( "Invalid column reference: " + column );
    }
    return i + 1;
  }

  @Override @NotSupported
  public boolean first() throws SQLException {
    throw new SQLFeatureNotSupportedException( "Scrollable resultsets are not supported" );
  }

  @Override
  public int getConcurrency() throws SQLException {
    return ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public String getCursorName() throws SQLException {
    return serviceName;
  }

  @Override
  public int getFetchDirection() throws SQLException {
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public int getFetchSize() throws SQLException {
    return 1;
  }

  @Override
  public int getHoldability() throws SQLException {
    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return new ThinResultSetMetaData( serviceName, rowMeta );
  }

  @Override
  public int getRow() throws SQLException {
    return rowNumber;
  }

  @Override
  public Statement getStatement() throws SQLException {
    return statement;
  }

  @Override
  public int getType() throws SQLException {
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override @NotSupported
  public void insertRow() throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updating resultsets are not supported" );
  }

  @Override @NotSupported
  public boolean isAfterLast() throws SQLException {
    throw new SQLFeatureNotSupportedException( "Scrollable resultsets are not supported" );
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    return dataInputStream != null && currentRow == null;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return dataInputStream == null && currentRow == null;
  }

  @Override
  public boolean isFirst() throws SQLException {
    return rowNumber == 0;
  }

  @Override
  public boolean isLast() throws SQLException {
    return dataInputStream == null && currentRow != null;
  }

  @Override @NotSupported
  public boolean last() throws SQLException {
    throw new SQLFeatureNotSupportedException( "Scrollable resultsets are not supported" );
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
  }

  @Override @NotSupported
  public void moveToInsertRow() throws SQLException {
    throw new SQLFeatureNotSupportedException( "Scrollable resultsets are not supported" );
  }

  @Override
  public boolean next() throws SQLException {
    if ( dataInputStream == null ) {
      return false;
    }

    try {
      currentRow = rowMeta.readData( dataInputStream );
      rowNumber++;
      return true;
    } catch ( KettleEOFException e ) {
      dataInputStream = null;
      return false;
    } catch ( Exception e ) {
      throw new SQLException( e );
    }
  }

  @Override @NotSupported
  public boolean previous() throws SQLException {
    throw new SQLFeatureNotSupportedException( "Scrollable resultsets are not supported" );
  }

  @Override
  public void refreshRow() throws SQLException {
  }

  @Override @NotSupported
  public boolean relative( int rowNumber ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Scrollable resultsets are not supported" );
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    return false;
  }

  @Override
  public boolean rowInserted() throws SQLException {
    return false;
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    return false;
  }

  @Override
  public void setFetchDirection( int direction ) throws SQLException {
  }

  @Override
  public void setFetchSize( int direction ) throws SQLException {
  }

  @Override
  public boolean wasNull() throws SQLException {
    return lastNull;
  }

  // Here are the getters...

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
    return getValue( index, new ValueRetriever<Double>() {
      @Override public Double value( int index ) throws Exception {
        return rowMeta.getNumber( currentRow, index );
      }
    } );
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
    return getValue( index, new ValueRetriever<Boolean>() {
      @Override public Boolean value( int index ) throws Exception {
        return rowMeta.getBoolean( currentRow, index );
      }
    } );
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
    return getValue( index, new ValueRetriever<Long>() {
      @Override public Long value( int index ) throws Exception {
        return rowMeta.getInteger( currentRow, index );
      }
    } );
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
  public Object getObject( int index ) throws SQLException {
    return getValue( index, new ValueRetriever<Object>() {
      @Override public Object value( int index ) throws Exception {
        return currentRow[index];
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

  // Update section below: all not supported...

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
  public void updateDate( int arg0, java.sql.Date arg1 ) throws SQLException {
    throw new SQLFeatureNotSupportedException( "Updates are not supported" );
  }

  @Override @NotSupported
  public void updateDate( String arg0, java.sql.Date arg1 ) throws SQLException {
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

  /**
   * @return the serviceName
   */
  public String getServiceName() {
    return serviceName;
  }

  /**
   * @return the serviceTransName
   */
  public String getServiceTransName() {
    return serviceTransName;
  }

  /**
   * @return the serviceObjectId
   */
  public String getServiceObjectId() {
    return serviceObjectId;
  }

  /**
   * @return the sqlTransName
   */
  public String getSqlTransName() {
    return sqlTransName;
  }

  /**
   * @return the sqlObjectId
   */
  public String getSqlObjectId() {
    return sqlObjectId;
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

  private interface ValueRetriever<T> {
    T value( int index ) throws Exception;
  }
}
