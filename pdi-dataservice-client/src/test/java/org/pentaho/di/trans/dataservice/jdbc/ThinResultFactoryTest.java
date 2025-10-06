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
    IOException expected = new IOException();
    InputStream inputStream = new InputStream() {
      @Override
      public int read() throws IOException {
        throw expected;
      }
    };

    try {
      assertThat( factory.loadHeader( new DataInputStream( inputStream ) ), not( anything() ) );
    } catch ( SQLException e ) {
      assertThat( Throwables.getRootCause( e ), is( (Throwable) expected ) );
    }
  }
}
