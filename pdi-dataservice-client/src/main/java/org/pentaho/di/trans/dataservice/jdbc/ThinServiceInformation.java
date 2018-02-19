/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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
