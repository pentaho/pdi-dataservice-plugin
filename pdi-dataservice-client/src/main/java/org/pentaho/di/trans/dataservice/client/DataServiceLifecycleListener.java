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

import org.pentaho.di.core.annotations.LifecyclePlugin;
import org.pentaho.di.core.lifecycle.LifeEventHandler;
import org.pentaho.di.core.lifecycle.LifecycleException;
import org.pentaho.di.core.lifecycle.LifecycleListener;
import org.pentaho.di.trans.dataservice.jdbc.ThinConnection;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@LifecyclePlugin( id = "DataServiceLifecycleListener" )
public class DataServiceLifecycleListener implements LifecycleListener {

  private final AtomicReference<DataServiceClientService> dataServiceClientService =
    new AtomicReference<DataServiceClientService>();
  private final AtomicBoolean enabled = new AtomicBoolean( false );

  public void bind( DataServiceClientService service ) {
    dataServiceClientService.set( service );
    setup( service );
  }

  public void unbind( DataServiceClientService service ) {
    dataServiceClientService.set( null );
    setup( null );
  }

  @Override public void onStart( LifeEventHandler handler ) throws LifecycleException {
    if ( enabled.compareAndSet( false, true ) ) {
      setup( this.dataServiceClientService.get() );
    }
  }

  @Override public void onExit( LifeEventHandler handler ) throws LifecycleException {
    if ( enabled.compareAndSet( true, false ) ) {
      setup( this.dataServiceClientService.get() );
    }
  }

  private synchronized void setup( DataServiceClientService clientService ) {
    ThinConnection.localClient = enabled.get() ? clientService : null;
  }
}
