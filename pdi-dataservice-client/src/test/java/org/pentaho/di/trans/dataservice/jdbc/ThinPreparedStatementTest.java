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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.pentaho.di.trans.dataservice.client.DataServiceClientService;

import java.io.DataInputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author nhudak
 */
@RunWith( MockitoJUnitRunner.class )
public class ThinPreparedStatementTest extends JDBCTestBase<ThinPreparedStatement> {

  static final String SQL = "SELECT * FROM dataService WHERE query = ?";
  @Mock ThinConnection connection;
  @Mock DataServiceClientService clientService;
  ThinPreparedStatement statement;
  String lastQuery;

  public ThinPreparedStatementTest() {
    super( ThinPreparedStatement.class );
  }

  @Before
  public void setUp() throws Exception {
    statement = new ThinPreparedStatement( connection, SQL );
    when( connection.getClientService() ).thenReturn( clientService );
    when( clientService.query( anyString(), anyInt() ) ).then( new Answer<DataInputStream>() {
      @Override public DataInputStream answer( InvocationOnMock invocation ) throws Throwable {
        lastQuery = (String) invocation.getArguments()[0];
        return MockDataInput.dual().toDataInputStream();
      }
    } );
  }

  @Override protected ThinPreparedStatement getTestObject() {
    return statement;
  }

  protected void verifyQuery( String query ) throws SQLException {
    statement.executeQuery();
    assertThat( lastQuery, equalTo( "SELECT * FROM dataService WHERE query = " + query ) );
  }

  @Test
  public void testBigDecimal() throws Exception {
    statement.setBigDecimal( 1, BigDecimal.valueOf( 2000 ) );

    verifyQuery( "2000" );
  }

  @Test
  public void testBoolean() throws Exception {
    statement.setBoolean( 1, true );

    verifyQuery( "TRUE" );
  }

  @Test
  public void testByte() throws Exception {
    statement.setByte( 1, (byte) 16 );

    verifyQuery( "16" );
  }

  @Test
  public void testDates() throws Exception {
    long millis = System.currentTimeMillis();
    String expected = ThinPreparedStatement.FORMAT.format( new java.util.Date( millis ) );

    statement.setDate( 1, new Date( millis ) );
    verifyQuery( expected );

    statement.setTime( 1, new Time( millis ) );
    verifyQuery( expected );

    statement.setTimestamp( 1, new Timestamp( millis ) );
    verifyQuery( expected );

    verify( clientService, times( 3 ) ).query( anyString(), anyInt() );
  }

  @Test
  public void testDouble() throws Exception {
    statement.setDouble( 1, 2.6 );
    verifyQuery( "2.6" );
  }

  @Test
  public void testFloat() throws Exception {
    statement.setFloat( 1, 2.5f );
    verifyQuery( "2.5" );
  }

  @Test
  public void testInt() throws Exception {
    statement.setInt( 1, 42 );
    verifyQuery( "42" );
  }

  @Test
  public void testLong() throws Exception {
    statement.setLong( 1, 42 );
    verifyQuery( "42" );
  }

  @Test
  public void testNull() throws Exception {
    // TODO not all types translate correctly
    for ( Integer type : ImmutableList.of( Types.VARCHAR, Types.BOOLEAN, Types.BIGINT ) ) {
      statement.setNull( 1, type );
      verifyQuery( "NULL" );
      assertThat( statement.getParameterMetaData().getParameterType( 1 ), is( type ) );
    }
  }

  @Test
  public void testObject() throws Exception {
    ImmutableMap<? extends Serializable, String> tests = ImmutableMap.of(
      2.5f, "2.5",
      42, "42",
      "Some String", "'Some String'"
    );
    for ( Map.Entry<? extends Serializable, String> entry : tests.entrySet() ) {
      statement.setObject( 1, entry.getKey() );
      verifyQuery( entry.getValue() );
    }
  }

  @Test
  public void testString() throws Exception {
    statement.setString( 1, "foobar" );
    verifyQuery( "'foobar'" );
  }

  @Override protected Object mockValue( Class<?> type ) {
    if ( type.equals( URL.class ) ) {
      try {
        return new URL( "http://localhost:9080/pentaho-di" );
      } catch ( MalformedURLException e ) {
        throw new AssertionError( e );
      }
    }
    return super.mockValue( type );
  }
}
