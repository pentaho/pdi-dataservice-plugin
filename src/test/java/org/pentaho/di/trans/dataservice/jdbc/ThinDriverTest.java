package org.pentaho.di.trans.dataservice.jdbc;

import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Properties;

import static org.junit.Assert.*;

public class ThinDriverTest {

  private ThinDriver driver;

  @Before
  public void setUp() throws Exception {
    driver = new ThinDriver();
  }

  @Test
  public void testAcceptsURL() throws Exception {
    assertTrue( driver.acceptsURL( "jdbc:pdi://slaveserver:8181/kettle/?webappname=pdi" ) );
    assertFalse( driver.acceptsURL( "jdbc:mysql://localhost" ) );
  }

  @Test
  public void testConnectNull() throws Exception {
    Properties properties = new Properties();
    properties.setProperty( "user", "user" );
    properties.setProperty( "password", "password" );
    assertNull( driver.connect( "jdbc:mysql://localhost", properties ) );
  }

  @Test
  public void testConnectError() {
    Properties properties = new Properties();
    properties.setProperty( "user", "user" );
    properties.setProperty( "password", "password" );
    try {
      driver.connect( "jdbc:pdi://localhost", properties );
    } catch ( SQLException e ) {
      // Expected exception
      return;
    }
    fail();
  }
}
