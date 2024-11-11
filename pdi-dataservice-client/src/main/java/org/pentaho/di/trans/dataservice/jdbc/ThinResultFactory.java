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
