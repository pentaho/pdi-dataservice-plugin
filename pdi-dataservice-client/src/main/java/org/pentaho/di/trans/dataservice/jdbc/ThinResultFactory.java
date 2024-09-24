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

import com.google.common.base.Throwables;
import org.pentaho.di.core.KettleClientEnvironment;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;

import java.io.DataInputStream;
import java.sql.SQLException;

/**
 * @author nhudak
 */
public class ThinResultFactory {
  public ThinResultSet loadResultSet( DataInputStream dataInputStream, IDataServiceClientService client )
      throws SQLException {
    return new ThinResultSet( loadHeader( dataInputStream ), dataInputStream, client );
  }

  public ThinResultHeader loadHeader( DataInputStream dataInputStream ) throws SQLException {
    try {
      // Read the name of the service we're reading from
      //
      String serviceName = dataInputStream.readUTF();

      // Get some information about what's going on on the slave server
      //
      String serviceTransName = dataInputStream.readUTF();
      String serviceObjectId = dataInputStream.readUTF();
      String sqlTransName = dataInputStream.readUTF();
      String sqlObjectId = dataInputStream.readUTF();

      // Get the row metadata...
      //
      if ( !KettleClientEnvironment.isInitialized() ) {
        KettleClientEnvironment.init();
      }
      RowMeta rowMeta = new RowMeta( dataInputStream );
      return new ThinResultHeader( serviceName, serviceTransName, serviceObjectId, sqlTransName, sqlObjectId, rowMeta );
    } catch ( Exception e ) {
      Throwables.propagateIfPossible( e, SQLException.class );
      throw new SQLException( "Unable to load result set", e );
    }
  }
}
