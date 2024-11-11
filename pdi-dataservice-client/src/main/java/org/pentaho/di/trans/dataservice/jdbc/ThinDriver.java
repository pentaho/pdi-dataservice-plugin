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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

public class ThinDriver implements Driver {

  public static final String BASE_URL = "jdbc:pdi://";
  public static final String SERVICE_NAME = "/kettle";
  public static final String NAME = "PDI Data Services JDBC driver";

  protected static Logger logger = Logger.getLogger( NAME );

  static {
    new ThinDriver().register();
  }

  public ThinDriver() {
  }

  private void register() {
    try {
      DriverManager.registerDriver( this );
    } catch ( SQLException e ) {
      logger.throwing( DriverManager.class.getName(), "registerDriver", e );
    }
  }

  @Override
  public boolean acceptsURL( String url ) {
    return url.startsWith( BASE_URL );
  }

  @Override
  public Connection connect( String url, Properties properties ) throws SQLException {
    if ( acceptsURL( url ) ) {
      ThinConnection connection = createConnection( url, properties );
      if ( connection.isValid( 0 ) ) {
        return connection;
      } else {
        throw connection.getWarnings();
      }
    }
    return null;
  }

  protected ThinConnection createConnection( String url, Properties properties ) throws SQLException {
    ThinConnection thinConnection = new ThinConnection();
    ThinConnection.Builder builder = thinConnection.createBuilder();
    return builder
      .parseUrl( url )
      .readProperties( properties )
      .build();
  }

  @Override
  public int getMajorVersion() {
    return 6;
  }

  @Override
  public int getMinorVersion() {
    return 0;
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo( String arg0, Properties arg1 ) throws SQLException {
    return new DriverPropertyInfo[0];
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  public Logger getParentLogger() {
    return logger;
  }

}
