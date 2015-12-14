/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.client;

import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.dataservice.jdbc.ThinServiceInformation;
import org.pentaho.metastore.api.IMetaStore;

import java.io.DataInputStream;
import java.sql.SQLException;
import java.util.List;

public interface DataServiceClientService {
  /**
   * @deprecated as of 6.1.
   *
   * Implementations should locate the meta store. It does not need to be provided users of this interface.
   */
  @Deprecated
  void setMetaStore( IMetaStore metaStore );

  /**
   * @deprecated as of 6.1.
   *
   * Implementations should locate the repository. It does not need to be provided users of this interface.
   */
  @Deprecated
  void setRepository( Repository repository );

  DataInputStream query( String sql, int maxRows ) throws SQLException;
  List<ThinServiceInformation> getServiceInformation() throws SQLException;
}
