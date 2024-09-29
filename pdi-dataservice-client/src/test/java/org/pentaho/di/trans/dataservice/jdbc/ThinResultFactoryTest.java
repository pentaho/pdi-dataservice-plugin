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

@RunWith( org.mockito.junit.MockitoJUnitRunner.class )
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
