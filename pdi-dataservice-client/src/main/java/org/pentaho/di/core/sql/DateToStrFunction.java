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

/**
 * Information on a DATE_TO_STR( field-name, 'date-mask' ) call in a SQLCondition.
 */
public class DateToStrFunction {
  private String fieldName;
  private String dateMask;
  private String resultName;

  public DateToStrFunction( String field, String mask, String resultName ) {
    fieldName = field;
    dateMask = mask;
    this.resultName = resultName;
  }

  /**
   * @return the source field name
   */
  public String getFieldName() {
    return fieldName;
  }

  /**
   * @return the date format mask
   */
  public String getDateMask() {
    return dateMask;
  }

  /**
   * @return the temporary result field name
   */
  public String getResultName() {
    return resultName;
  }
}
