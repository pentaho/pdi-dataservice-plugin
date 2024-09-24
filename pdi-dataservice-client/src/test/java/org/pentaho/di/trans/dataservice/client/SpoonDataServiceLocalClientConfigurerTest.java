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

import com.google.common.base.Supplier;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.core.lifecycle.LifeEventHandler;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.stores.delegate.DelegatingMetaStore;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class SpoonDataServiceLocalClientConfigurerTest {
  SpoonDataServiceLocalClientConfigurer lifecycleListener;

  @Mock
  Spoon spoon;
  @Mock
  Supplier<Spoon> spoonSupplier;

  @Mock
  Repository repository;
  @Mock
  DelegatingMetaStore metaStore;

  @Before
  public void setUp() throws Exception {
    when( spoon.getRepository() ).thenReturn( repository );
    when( spoon.getMetaStore() ).thenReturn( metaStore );
    when( spoonSupplier.get() ).thenReturn( spoon );

    lifecycleListener = new SpoonDataServiceLocalClientConfigurer( spoonSupplier );
  }

  @Test
  public void testDataServiceClientServiceIsConfiguredAtStartWhenBindingIsBeforeStart() throws Exception {
    IDataServiceClientService clientService = mock( IDataServiceClientService.class );
    lifecycleListener.bind( clientService );

    verify( clientService, never() ).setRepository( any( Repository.class ) );
    verify( clientService, never() ).setMetaStore( any( IMetaStore.class ) );

    lifecycleListener.onStart( mock( LifeEventHandler.class ) );

    verify( clientService, times( 1 ) ).setRepository( any( Repository.class ) );
    verify( clientService, times( 1 ) ).setMetaStore( any( IMetaStore.class ) );
  }

  @Test
  public void testDataServiceClientServiceIsConfiguredWhenBindingIsAfterStart() throws Exception {
    lifecycleListener.onStart( mock( LifeEventHandler.class ) );

    IDataServiceClientService clientService = mock( IDataServiceClientService.class );
    lifecycleListener.bind( clientService );

    verify( clientService, times( 1 ) ).setRepository( any( Repository.class ) );
    verify( clientService, times( 1 ) ).setMetaStore( any( IMetaStore.class ) );
  }

  @Test
  public void testDataServiceClientServiceNotConfiguredIfBindingIsAfterExit() throws Exception {
    lifecycleListener.onStart( mock( LifeEventHandler.class ) );
    lifecycleListener.onExit( mock( LifeEventHandler.class ) );

    IDataServiceClientService clientService = mock( IDataServiceClientService.class );
    lifecycleListener.bind( clientService );

    verify( clientService, never() ).setRepository( any( Repository.class ) );
    verify( clientService, never() ).setMetaStore( any( IMetaStore.class ) );
  }

  @Test
  public void testSpoonSupplierNotCalledIfNoDataServiceClientServiceIsBound() throws Exception {
    lifecycleListener.onStart( mock( LifeEventHandler.class ) );

    // Spoon instance is not requested until onStart has been called and service is bound
    verify( spoonSupplier, never() ).get();
  }

  @Test
  public void testSpoonSupplierNotCalledIfNotStarted() throws Exception {
    IDataServiceClientService clientService = mock( IDataServiceClientService.class );
    lifecycleListener.bind( clientService );

    // Spoon instance is not requested until onStart has been called and service is bound
    verify( spoonSupplier, never() ).get();
  }

  @Test
  public void testSpoonSupplierCalledAtStartWhenDataServiceClientServiceIsBound() throws Exception {
    IDataServiceClientService clientService = mock( IDataServiceClientService.class );
    lifecycleListener.bind( clientService );

    verify( spoonSupplier, never() ).get();

    lifecycleListener.onStart( mock( LifeEventHandler.class ) );

    verify( spoonSupplier, atLeastOnce() ).get();
  }

  @Test
  public void testSpoonSupplierCalledDataServiceClientServiceBindingWhenStarted() throws Exception {
    lifecycleListener.onStart( mock( LifeEventHandler.class ) );

    verify( spoonSupplier, never() ).get();

    IDataServiceClientService clientService = mock( IDataServiceClientService.class );
    lifecycleListener.bind( clientService );

    // Spoon instance is not requested until onStart has been called and service is bound
    verify( spoonSupplier, atLeastOnce() ).get();
  }

}
