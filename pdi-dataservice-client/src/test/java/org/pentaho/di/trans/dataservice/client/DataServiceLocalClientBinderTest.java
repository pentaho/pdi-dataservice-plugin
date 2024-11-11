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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;
import org.pentaho.di.trans.dataservice.jdbc.ThinConnection;

import static junit.framework.TestCase.assertNotSame;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

@RunWith( MockitoJUnitRunner.class )
public class DataServiceLocalClientBinderTest {
  DataServiceLocalClientBinder binder;

  @Before
  public void setUp() throws Exception {
    binder = new DataServiceLocalClientBinder();
  }

  @Test
  public void testNullBind() throws Exception {
    binder.bind( null );

    assertNull( ThinConnection.localClient );
  }

  @Test
  public void testBind() throws Exception {
    IDataServiceClientService clientService = mock( IDataServiceClientService.class );
    binder.bind( clientService );

    assertSame( ThinConnection.localClient, clientService );
  }

  @Test
  public void testReBind() throws Exception {
    IDataServiceClientService clientService = mock( IDataServiceClientService.class );
    binder.bind( clientService );

    assertSame( ThinConnection.localClient, clientService );

    IDataServiceClientService anotherClientService = mock( IDataServiceClientService.class );
    binder.bind( anotherClientService );

    assertNotSame( ThinConnection.localClient, clientService );
    assertSame( ThinConnection.localClient, anotherClientService );
  }

  @Test
  public void testUnbind() throws Exception {
    IDataServiceClientService clientService = mock( IDataServiceClientService.class );
    binder.bind( clientService );

    assertSame( ThinConnection.localClient, clientService );

    binder.unbind( clientService );

    assertThat( ThinConnection.localClient, nullValue() );
  }

  @Test
  public void testUnbindDifferentReference() throws Exception {
    IDataServiceClientService clientService = mock( IDataServiceClientService.class );
    binder.bind( clientService );

    assertSame( ThinConnection.localClient, clientService );

    IDataServiceClientService anotherClientService = mock( IDataServiceClientService.class );
    binder.unbind( anotherClientService );

    assertNotSame( ThinConnection.localClient, anotherClientService );
    assertSame( ThinConnection.localClient, clientService );
  }

  @Test
  public void testUnbindNull() throws Exception {
    IDataServiceClientService clientService = mock( IDataServiceClientService.class );
    binder.bind( clientService );

    assertSame( ThinConnection.localClient, clientService );

    binder.unbind( null );

    assertNotSame( ThinConnection.localClient, null );
    assertSame( ThinConnection.localClient, clientService );
  }

}
