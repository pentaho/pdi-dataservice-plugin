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
