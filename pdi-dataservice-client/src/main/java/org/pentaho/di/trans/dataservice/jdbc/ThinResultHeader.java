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

import org.pentaho.di.core.row.RowMetaInterface;

public class ThinResultHeader {
  private final String serviceName;
  private final String serviceTransName;
  private final String serviceObjectId;
  private final String sqlTransName;
  private final String sqlObjectId;
  private final RowMetaInterface rowMeta;

  public ThinResultHeader( String serviceName, String serviceTransName, String serviceObjectId, String sqlTransName,
                           String sqlObjectId, RowMetaInterface rowMeta ) {
    this.serviceName = serviceName;
    this.serviceTransName = serviceTransName;
    this.serviceObjectId = serviceObjectId;
    this.sqlTransName = sqlTransName;
    this.sqlObjectId = sqlObjectId;
    this.rowMeta = rowMeta;
  }

  /**
   * @return the serviceName
   */
  public String getServiceName() {
    return serviceName;
  }

  /**
   * @return the serviceTransName
   */
  public String getServiceTransName() {
    return serviceTransName;
  }

  /**
   * @return the serviceObjectId
   */
  public String getServiceObjectId() {
    return serviceObjectId;
  }

  /**
   * @return the sqlTransName
   */
  public String getSqlTransName() {
    return sqlTransName;
  }

  /**
   * @return the sqlObjectId
   */
  public String getSqlObjectId() {
    return sqlObjectId;
  }

  public RowMetaInterface getRowMeta() {
    return rowMeta;
  }
}
