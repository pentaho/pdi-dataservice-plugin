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


package org.pentaho.di.trans.dataservice.client;

import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;
import org.pentaho.di.trans.dataservice.jdbc.ThinConnection;

import java.util.concurrent.atomic.AtomicReference;

public class DataServiceLocalClientBinder {

  private final AtomicReference<IDataServiceClientService> dataServiceClientService = new AtomicReference<IDataServiceClientService>();

  public void bind( IDataServiceClientService service ) {
    dataServiceClientService.set( service );

    ThinConnection.localClient = service;
  }

  public void unbind( IDataServiceClientService service ) {
    if ( dataServiceClientService.compareAndSet( service, null ) ) {
      ThinConnection.localClient = null;
    }
  }

}
