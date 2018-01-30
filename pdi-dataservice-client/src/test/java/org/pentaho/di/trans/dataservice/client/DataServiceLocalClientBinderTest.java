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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
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
