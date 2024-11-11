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
