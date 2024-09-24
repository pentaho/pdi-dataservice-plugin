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

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Wrapper;

/**
 * @author nhudak
 */
public class ThinBase implements Wrapper {
  protected SQLWarning warning;

  @Override
  public boolean isWrapperFor( Class<?> type ) throws SQLException {
    return type.isInstance( this );
  }

  @Override
  public <T> T unwrap( Class<T> type ) throws SQLException {
    if ( isWrapperFor( type ) ) {
      return type.cast( this );
    }
    throw new SQLException( "Not a wrapper for " + type );
  }

  public void clearWarnings() throws SQLException {
    warning = null;
  }

  public SQLWarning getWarnings() throws SQLException {
    return warning;
  }

  protected void setWarning( Exception e ) {
    warning = new SQLWarning( e );
  }
}
