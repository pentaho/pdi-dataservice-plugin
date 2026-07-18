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



package org.pentaho.di.core.jdbc;

public class FoundClause {
  private final String clause;
  private final String rest;

  public FoundClause( String clause, String rest ) {
    this.clause = clause;
    this.rest = rest;
  }

  public String getClause() {
    return clause;
  }

  public String getRest() {
    return rest;
  }
}
