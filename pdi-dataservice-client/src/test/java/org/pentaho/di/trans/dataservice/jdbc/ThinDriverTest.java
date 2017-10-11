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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Properties;

import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class ThinDriverTest {

  public static final String URL = "jdbc:pdi://slaveserver:8181/kettle/?webappname=pdi";
  private ThinDriver driver;
  private @Mock ThinConnection connection;
  private Properties properties;

  @Before
  public void setUp() throws Exception {
    driver = new ThinDriver() {
      @Override protected ThinConnection createConnection( String url, Properties properties ) throws SQLException {
        assertThat( url, is( URL ) );
        assertThat( properties, equalTo( ThinDriverTest.this.properties ) );

        // Ensure no exceptions thrown by real method
        super.createConnection( url, properties );

        // Inject mock
        return connection;
      }
    };

    properties = new Properties();
    properties.setProperty( "user", "user" );
    properties.setProperty( "password", "password" );
  }

  @Test
  public void testAcceptsURL() throws Exception {
    assertTrue( driver.acceptsURL( URL ) );
    assertFalse( driver.acceptsURL( "jdbc:mysql://localhost" ) );
  }

  @Test
  public void testConnectNull() throws Exception {
    assertNull( driver.connect( "jdbc:mysql://localhost", properties ) );
    verify( connection, never() ).isValid( anyInt() );
  }

  @Test
  public void testDriverProperties() throws Exception {
    assertThat( driver.getMajorVersion(), greaterThan( 0 ) );
    assertThat( driver.getMinorVersion(), greaterThanOrEqualTo( 0 ) );
    assertThat( driver.getPropertyInfo( URL, properties ), emptyArray() );
    assertThat( driver.jdbcCompliant(), is( false ) );
    assertThat( driver.getParentLogger(), sameInstance( ThinDriver.logger ) );
  }

  @Test
  public void testConnect() throws Exception {
    when( connection.isValid( anyInt() ) ).thenReturn( true );

    assertThat( driver.connect( URL, properties ), sameInstance( (Connection) connection ) );
  }

  @Test
  public void testConnectError() throws Exception {
    SQLWarning expected = new SQLWarning( "expected" );
    when( connection.isValid( anyInt() ) ).thenReturn( false );
    when( connection.getWarnings() ).thenReturn( expected );
    try {
      driver.connect( URL, properties );
      fail( "Expected exception" );
    } catch ( SQLException e ) {
      assertThat( e, sameInstance( (SQLException) expected ) );
    }
  }
}
