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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;

import java.io.DataInputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author nhudak
 */
@RunWith( MockitoJUnitRunner.class )
public class ThinPreparedStatementTest extends JDBCTestBase<ThinPreparedStatement> {

  static final String SQL = "SELECT * FROM dataService WHERE query = ?";
  @Mock ThinConnection connection;
  @Mock IDataServiceClientService clientService;
  @Mock ThinResultFactory resultFactory;
  @Mock ThinResultSet resultSet;
  @Mock ThinResultSet resultSetStreaming;
  @Mock ThinResultSetMetaData resultSetMetaData;
  @Mock ThinResultSetMetaData resultSetMetaDataStreaming;
  @Mock ImmutableMap<String, String> mockParameters;
  IDataServiceClientService.StreamingMode mockWindowType = IDataServiceClientService.StreamingMode.ROW_BASED;
  long windowSize = 10;
  long windowEvery = 1;
  long windowLimit = 1000;
  ThinPreparedStatement statement;

  public ThinPreparedStatementTest() {
    super( ThinPreparedStatement.class );
  }

  @Before
  public void setUp() throws Exception {
    statement = new ThinPreparedStatement( connection, SQL, resultFactory );
    when( connection.getClientService() ).thenReturn( clientService );
    when( connection.getParameters() ).thenReturn( mockParameters );

    DataInputStream dataInputStream = MockDataInput.dual().toDataInputStream();
    DataInputStream dataInputStreamStreaming = MockDataInput.dual().toDataInputStream();
    when( clientService.query( anyString(), anyInt(), anyMap() ) ).thenReturn( dataInputStream );
    when( clientService.query( anyString(),anyObject(), anyLong(), anyLong(), anyLong(), anyMap() ) )
            .thenReturn( dataInputStreamStreaming );
    when( resultFactory.loadResultSet( same( dataInputStream ), same( clientService ) ) ).thenReturn( resultSet );
    when( resultFactory.loadResultSet( same( dataInputStreamStreaming ), same( clientService ) ) )
            .thenReturn( resultSetStreaming );
    when( resultSet.getMetaData() ).thenReturn( resultSetMetaData );
    when( resultSetStreaming.getMetaData() ).thenReturn( resultSetMetaDataStreaming );
  }

  @Override protected ThinPreparedStatement getTestObject() {
    return statement;
  }

  protected void verifyQuery( String query ) throws SQLException {
    assertThat( statement.getResultSet(), nullValue() );
    assertThat( statement.getMetaData(), nullValue() );
    statement.executeQuery();
    verify( clientService ).query( "SELECT * FROM dataService WHERE query = " + query, -1, mockParameters );
    assertThat( statement.getResultSet(), is( (ResultSet) resultSet ) );
    assertThat( statement.getMetaData(), is( (ResultSetMetaData) resultSetMetaData ) );
  }

  protected void verifyStreamingQuery( String query ) throws SQLException {
    assertThat( statement.getResultSet(), nullValue() );
    assertThat( statement.getMetaData(), nullValue() );
    statement.executeQuery( mockWindowType, windowSize, windowEvery, windowLimit );
    verify( clientService ).query( "SELECT * FROM dataService WHERE query = " + query, mockWindowType,
            windowSize, windowEvery, windowLimit, mockParameters );
    assertThat( statement.getResultSet(), is( (ResultSet) resultSetStreaming ) );
    assertThat( statement.getMetaData(), is( (ResultSetMetaData) resultSetMetaDataStreaming ) );
  }

  @Test
  public void testBigDecimal() throws Exception {
    statement.setBigDecimal( 1, BigDecimal.valueOf( 2000 ) );

    verifyQuery( "2000" );
  }

  @Test
  public void testBigDecimalStreaming() throws Exception {
    statement.setBigDecimal( 1, BigDecimal.valueOf( 2000 ) );

    verifyStreamingQuery( "2000" );
  }

  @Test
  public void testBoolean() throws Exception {
    statement.setBoolean( 1, true );

    verifyQuery( "TRUE" );
  }

  @Test
  public void testBooleanStreaming() throws Exception {
    statement.setBoolean( 1, true );

    verifyStreamingQuery( "TRUE" );
  }

  @Test
  public void testByte() throws Exception {
    statement.setByte( 1, (byte) 16 );

    verifyQuery( "16" );
  }

  @Test
  public void testByteStreaming() throws Exception {
    statement.setByte( 1, (byte) 16 );

    verifyStreamingQuery( "16" );
  }

  @Test
  public void testDate() throws Exception {
    long millis = System.currentTimeMillis();
    String expected = ThinPreparedStatement.FORMAT.format( new Date( millis ) );

    statement.setDate( 1, new Date( millis ) );
    verifyQuery( expected );
  }

  @Test
  public void testDateStreaming() throws Exception {
    long millis = System.currentTimeMillis();
    String expected = ThinPreparedStatement.FORMAT.format( new Date( millis ) );

    statement.setDate( 1, new Date( millis ) );
    verifyStreamingQuery( expected );
  }

  @Test
  public void testTime() throws Exception {
    long millis = System.currentTimeMillis();
    String expected = ThinPreparedStatement.FORMAT.format( new Time( millis ) );

    statement.setTime( 1, new Time( millis ) );
    verifyQuery( expected );
  }

  @Test
  public void testTimeStreaming() throws Exception {
    long millis = System.currentTimeMillis();
    String expected = ThinPreparedStatement.FORMAT.format( new Time( millis ) );

    statement.setTime( 1, new Time( millis ) );
    verifyStreamingQuery( expected );
  }

  @Test
  public void testTimestamp() throws Exception {
    long millis = System.currentTimeMillis();
    String expected = ThinPreparedStatement.FORMAT.format( new Timestamp( millis ) );

    statement.setTimestamp( 1, new Timestamp( millis ) );
    verifyQuery( expected );
  }

  @Test
  public void testTimestampStreaming() throws Exception {
    long millis = System.currentTimeMillis();
    String expected = ThinPreparedStatement.FORMAT.format( new Timestamp( millis ) );

    statement.setTimestamp( 1, new Timestamp( millis ) );
    verifyStreamingQuery( expected );
  }

  @Test
  public void testDouble() throws Exception {
    statement.setDouble( 1, 2.6 );
    verifyQuery( "2.6" );
  }

  @Test
  public void testDoubleStreaming() throws Exception {
    statement.setDouble( 1, 2.6 );
    verifyStreamingQuery( "2.6" );
  }

  @Test
  public void testFloat() throws Exception {
    statement.setFloat( 1, 2.5f );
    verifyQuery( "2.5" );
  }

  @Test
  public void testFloatStreaming() throws Exception {
    statement.setFloat( 1, 2.5f );
    verifyStreamingQuery( "2.5" );
  }

  @Test
  public void testInt() throws Exception {
    statement.setInt( 1, 42 );
    verifyQuery( "42" );
  }

  @Test
  public void testIntStreaming() throws Exception {
    statement.setInt( 1, 42 );
    verifyStreamingQuery( "42" );
  }

  @Test
  public void testLong() throws Exception {
    statement.setLong( 1, 42 );
    verifyQuery( "42" );
  }

  @Test
  public void testLongStreaming() throws Exception {
    statement.setLong( 1, 42 );
    verifyStreamingQuery( "42" );
  }

  @Test
  public void testNull() throws Exception {
    // TODO not all types translate correctly
    for ( Integer type : ImmutableList.of( Types.VARCHAR, Types.BOOLEAN, Types.BIGINT ) ) {
      statement.setNull( 1, type );
      assertThat( statement.getParameterMetaData().getParameterType( 1 ), is( type ) );
    }
    verifyQuery( "NULL" );
  }

  @Test
  public void testNullStreaming() throws Exception {
    // TODO not all types translate correctly
    for ( Integer type : ImmutableList.of( Types.VARCHAR, Types.BOOLEAN, Types.BIGINT ) ) {
      statement.setNull( 1, type );
      assertThat( statement.getParameterMetaData().getParameterType( 1 ), is( type ) );
    }
    verifyStreamingQuery( "NULL" );
  }

  @Test
  public void testObject() throws Exception {
    ImmutableMap<? extends Serializable, Integer> tests = ImmutableMap.of(
      2.5f, Types.DOUBLE,
      42, Types.BIGINT,
      "Some String", Types.VARCHAR
    );
    for ( Map.Entry<? extends Serializable, Integer> entry : tests.entrySet() ) {
      statement.setObject( 1, entry.getKey() );
      assertThat( statement.getParameterMetaData().getParameterType( 1 ), is( entry.getValue() ) );
    }
  }

  @Test
  public void testNullObject() throws SQLException {
    statement.setObject( 1, null );
    assertThat( statement.getParameterMetaData().getParameterType( 1 ), is( Types.OTHER ) );
    assertThat( statement.getParamData()[0], is( nullValue() )  );
  }

  @Test
  public void testSetNull() throws SQLException {
    statement.setNull( 1, Types.OTHER );
    assertThat( statement.getParameterMetaData().getParameterType( 1 ), is( Types.OTHER ) );
    assertThat( statement.getParamData()[0], is( nullValue() )  );
  }

  @Test
  public void testString() throws Exception {
    statement.setString( 1, "foobar" );
    verifyQuery( "'foobar'" );
  }

  @Test
  public void testStringStreaming() throws Exception {
    statement.setString( 1, "foobar" );
    verifyStreamingQuery( "'foobar'" );
  }

  @Override protected Object mockValue( Class<?> type ) {
    if ( type.equals( URL.class ) ) {
      try {
        return new URL( "http://localhost:8080/pentaho" );
      } catch ( MalformedURLException e ) {
        throw new AssertionError( e );
      }
    }
    return super.mockValue( type );
  }
}
