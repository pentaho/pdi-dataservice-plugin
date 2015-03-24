package org.pentaho.di.trans.dataservice.client;

import org.pentaho.di.core.annotations.LifecyclePlugin;
import org.pentaho.di.core.lifecycle.LifeEventHandler;
import org.pentaho.di.core.lifecycle.LifecycleException;
import org.pentaho.di.core.lifecycle.LifecycleListener;
import org.pentaho.di.trans.dataservice.jdbc.ThinConnection;
import org.pentaho.di.ui.spoon.Spoon;

@LifecyclePlugin( id = "DataServiceLifecycleListener" )
public class DataServiceLifecycleListener implements LifecycleListener {

  private Spoon spoon = null;
  private DataServiceClientService dataServiceClientService = null;

  public void bind( DataServiceClientService service ) {
    dataServiceClientService = service;
    if ( spoon != null ) {
      setup();
    }
  }

  public void unbind( DataServiceClientService service ) {
    ThinConnection.localClient = null;
  }

  @Override public void onStart( LifeEventHandler handler ) throws LifecycleException {
    spoon = Spoon.getInstance();
    if ( dataServiceClientService != null ) {
      setup();
    }
  }

  @Override public void onExit( LifeEventHandler handler ) throws LifecycleException {

  }

  private void setup() {
    dataServiceClientService.setRepository( spoon.getRepository() );
    dataServiceClientService.setMetaStore( spoon.getMetaStore() );
    ThinConnection.localClient = dataServiceClientService;
  }
}