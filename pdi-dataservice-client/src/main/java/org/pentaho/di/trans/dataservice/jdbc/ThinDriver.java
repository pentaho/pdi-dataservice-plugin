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
