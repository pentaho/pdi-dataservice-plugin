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


package org.pentaho.di.core.sql;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleSQLException;

public class SQLLimit {

  private String limitClause;

  private int limit;
  private int offset;

  public SQLLimit( String limitClause ) throws KettleSQLException {
    this.limitClause = limitClause;

    parse();
  }

  /**
   *
   * @return The limit of rows to return
   */
  public int getLimit() {
    return limit;
  }

  /**
   *
   * @param limit The limit of rows to return
   */
  public void setLimit( int limit ) {
    this.limit = limit;
  }

  /**
   *
   * @return The offset of the rows to return
   */
  public int getOffset() {
    return offset;
  }

  /**
   *
   * @param offset The offset of the rows to return
   */
  public void setOffset( int offset ) {
    this.offset = offset;
  }

  private void parse() throws KettleSQLException {
    if ( Const.isEmpty( limitClause ) ) {
      return;
    }

    limitClause = limitClause.replaceAll( "\\s+", " " );

    if ( limitClause.contains( "," ) ) {
      String[] limitSplit = limitClause.split( "," );
      if ( limitSplit.length == 2 ) {
        offset = Integer.valueOf( limitSplit[ 0 ].trim() );
        limit = Integer.valueOf( limitSplit[ 1 ].trim() );
      }
      return;
    }

    if ( limitClause.toUpperCase().contains( "OFFSET" ) ) {
      String[] limitSplit = limitClause.split( " " );
      if ( limitSplit.length == 3 ) {
        offset = Integer.valueOf( limitSplit[ 2 ].trim() );
        limit = Integer.valueOf( limitSplit[ 0 ].trim() );
      }
      return;
    }

    try {
      limit = Integer.valueOf( limitClause.trim() );
      offset = 0;
    } catch ( NumberFormatException nfe ) {
      throw new KettleSQLException( "Invalid limit parameter in : " + limitClause );
    }
  }
}
