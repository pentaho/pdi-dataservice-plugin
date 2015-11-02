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

import com.google.common.base.Supplier;
import org.pentaho.di.core.annotations.LifecyclePlugin;
import org.pentaho.di.core.lifecycle.LifeEventHandler;
import org.pentaho.di.core.lifecycle.LifecycleException;
import org.pentaho.di.core.lifecycle.LifecycleListener;
import org.pentaho.di.trans.dataservice.jdbc.ThinConnection;
import org.pentaho.di.ui.spoon.Spoon;

import java.util.concurrent.atomic.AtomicReference;

@LifecyclePlugin( id = "DataServiceLifecycleListener" )
public class DataServiceLifecycleListener implements LifecycleListener {

  private final Supplier<Spoon> spoonSupplier;
  private final AtomicReference<DataServiceClientService> dataServiceClientService =
    new AtomicReference<DataServiceClientService>();

  public DataServiceLifecycleListener() {
    this( new Supplier<Spoon>() {
      @Override public Spoon get() {
        return Spoon.getInstance();
      }
    } );
  }

  public DataServiceLifecycleListener( Supplier<Spoon> spoonSupplier ) {
    this.spoonSupplier = spoonSupplier;
  }

  public void bind( DataServiceClientService service ) {
    dataServiceClientService.set( service );
    setup();
  }

  public void unbind( DataServiceClientService service ) {
    ThinConnection.localClient = null;
  }

  @Override public void onStart( LifeEventHandler handler ) throws LifecycleException {
    setup();
  }

  @Override public void onExit( LifeEventHandler handler ) throws LifecycleException {
  }

  private void setup() {
    DataServiceClientService dataServiceClientService = this.dataServiceClientService.get();
    if ( dataServiceClientService == null ) {
      return;
    }
    Spoon spoon = spoonSupplier.get();

    dataServiceClientService.setRepository( spoon.getRepository() );
    dataServiceClientService.setMetaStore( spoon.getMetaStore() );
    ThinConnection.localClient = dataServiceClientService;
  }
}
