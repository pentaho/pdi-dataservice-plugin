/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.di.trans.dataservice.jdbc;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.dataservice.jdbc.api.IThinServiceInformation;

public class ThinServiceInformation implements IThinServiceInformation {
  private String name;
  private boolean isStreaming;
  private RowMetaInterface serviceFields;

  public ThinServiceInformation( String name, boolean isStreaming, RowMetaInterface serviceFields ) {
    this.name = name;
    this.isStreaming = isStreaming;
    this.serviceFields = serviceFields;
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @return a boolean indicating if the data service is a streaming data service
   */
  public boolean isStreaming() {
    return isStreaming;
  }

  /**
   * @return the serviceFields
   */
  public RowMetaInterface getServiceFields() {
    return serviceFields;
  }

}
