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



package org.pentaho.di.core.sql;

public enum SQLAggregation {
  SUM( "SUM" ), AVG( "AVG" ), MIN( "MIN" ), MAX( "MAX" ), COUNT( "COUNT" );

  private String keyWord;

  private SQLAggregation( String keyWord ) {
    this.keyWord = keyWord;
  }

  public String getKeyWord() {
    return keyWord;
  }
}
