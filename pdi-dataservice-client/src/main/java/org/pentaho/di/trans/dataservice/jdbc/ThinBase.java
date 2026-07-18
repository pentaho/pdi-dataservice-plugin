/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
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
