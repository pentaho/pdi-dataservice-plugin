/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2017-2018 by Hitachi Vantara : http://www.pentaho.com
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
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;
import org.pentaho.di.ui.spoon.Spoon;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@LifecyclePlugin( id = "SpoonDataServiceLocalClientConfigurer" )
public class SpoonDataServiceLocalClientConfigurer implements LifecycleListener {

  private final Supplier<Spoon> spoonSupplier;
  private final AtomicReference<IDataServiceClientService> dataServiceClientService =
      new AtomicReference<IDataServiceClientService>();
  private final AtomicBoolean enabled = new AtomicBoolean( false );

  public SpoonDataServiceLocalClientConfigurer() {
    this( new Supplier<Spoon>() {
      @Override
      public Spoon get() {
        return Spoon.getInstance();
      }
    } );
  }

  public SpoonDataServiceLocalClientConfigurer( Supplier<Spoon> spoonSupplier ) {
    this.spoonSupplier = spoonSupplier;
  }

  public void bind( IDataServiceClientService service ) {
    dataServiceClientService.set( service );

    setup( service );
  }

  public void unbind( IDataServiceClientService service ) {
    dataServiceClientService.set( null );

    setup( null );
  }

  @Override
  public void onStart( LifeEventHandler handler ) throws LifecycleException {
    if ( enabled.compareAndSet( false, true ) ) {
      setup( this.dataServiceClientService.get() );
    }
  }

  @Override
  public void onExit( LifeEventHandler handler ) throws LifecycleException {
    if ( enabled.compareAndSet( true, false ) ) {
      setup( this.dataServiceClientService.get() );
    }
  }

  private synchronized void setup( IDataServiceClientService clientService ) {
    if ( enabled.get() && clientService != null ) {
      Spoon spoon = spoonSupplier.get();

      clientService.setRepository( spoon.getRepository() );
      clientService.setMetaStore( spoon.getMetaStore() );
    }
    // else, should we clean (set to null) the clientService repository and metastore?
  }
}
