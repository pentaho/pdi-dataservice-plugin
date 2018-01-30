/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.jdbc;

import com.google.common.base.Throwables;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author nhudak
 */

@RunWith( org.mockito.runners.MockitoJUnitRunner.class )
public class ThinResultFactoryTest {

  @Mock IDataServiceClientService client;

  @InjectMocks ThinResultFactory factory;

  @Test
  public void testLoadResultSet() throws Exception {
    DataInputStream inputStream = MockDataInput.dual().toDataInputStream();

    ThinResultSet resultSet = factory.loadResultSet( inputStream, client );
    ResultSetMetaData metaData = resultSet.getMetaData();

    assertThat( metaData.getTableName( 1 ), is( "dual" ) );
    assertThat( metaData.getColumnLabel( 1 ), is( "DUMMY" ) );
    assertThat( resultSet.next(), is( true ) );
    assertThat( resultSet.getString( 1 ), is( "x" ) );
  }

  @Test
  public void testLoadResultSetFailure() throws Exception {
    InputStream inputStream = mock( InputStream.class );
    IOException expected = new IOException();
    when( inputStream.read() ).thenThrow( expected );

    try {
      assertThat( factory.loadHeader( new DataInputStream( inputStream ) ), not( anything() ) );
    } catch ( SQLException e ) {
      assertThat( Throwables.getRootCause( e ), is( (Throwable) expected ) );
    }
  }
}
