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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;

import java.sql.SQLException;
import java.sql.Types;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.when;

/**
 * Created by rfellows on 9/23/16.
 */
@RunWith( MockitoJUnitRunner.class )
public class ThinResultSetMetaDataTest {
  ThinResultSetMetaData resultSetMetaData;

  @Mock RowMetaInterface rowMeta;
  @Mock ValueMetaInterface valueMeta;

  @Before
  public void setUp() throws Exception {
    when( rowMeta.getValueMeta( anyInt() ) ).thenReturn( valueMeta );
    resultSetMetaData = new ThinResultSetMetaData( "testing", rowMeta );
  }

  @Test
  public void getColumnType_boolean() throws Exception {
    when( valueMeta.getType() ).thenReturn( ValueMetaInterface.TYPE_BOOLEAN );
    assertEquals( Types.BIT, resultSetMetaData.getColumnType( 1 ) );
  }

  @Test
  public void getColumnType_string() throws Exception {
    when( valueMeta.getType() ).thenReturn( ValueMetaInterface.TYPE_STRING );
    assertEquals( Types.VARCHAR, resultSetMetaData.getColumnType( 1 ) );
  }

  @Test
  public void getColumnType_number() throws Exception {
    when( valueMeta.getType() ).thenReturn( ValueMetaInterface.TYPE_NUMBER );
    assertEquals( Types.DOUBLE, resultSetMetaData.getColumnType( 1 ) );
  }

  @Test
  public void getColumnType_timestamp() throws Exception {
    when( valueMeta.getType() ).thenReturn( ValueMetaInterface.TYPE_TIMESTAMP );
    assertEquals( Types.TIMESTAMP, resultSetMetaData.getColumnType( 1 ) );
  }

  @Test
  public void getColumnType_bignumber() throws Exception {
    when( valueMeta.getType() ).thenReturn( ValueMetaInterface.TYPE_BIGNUMBER );
    assertEquals( Types.DECIMAL, resultSetMetaData.getColumnType( 1 ) );
  }

  @Test
  public void getColumnType_integer() throws Exception {
    when( valueMeta.getType() ).thenReturn( ValueMetaInterface.TYPE_INTEGER );
    assertEquals( Types.BIGINT, resultSetMetaData.getColumnType( 1 ) );
  }

  @Test
  public void getColumnType_binary() throws Exception {
    when( valueMeta.getType() ).thenReturn( ValueMetaInterface.TYPE_BINARY );
    assertEquals( Types.BLOB, resultSetMetaData.getColumnType( 1 ) );
  }

  @Test
  public void getColumnType_inet() throws Exception {
    when( valueMeta.getType() ).thenReturn( ValueMetaInterface.TYPE_INET );
    assertEquals( Types.BINARY, resultSetMetaData.getColumnType( 1 ) );
  }

  @Test
  public void getColumnType_date() throws Exception {
    when( valueMeta.getType() ).thenReturn( ValueMetaInterface.TYPE_DATE );
    assertEquals( Types.TIMESTAMP, resultSetMetaData.getColumnType( 1 ) );
  }

  @Test( expected = SQLException.class )
  public void getColumnType_unknown() throws Exception {
    when( valueMeta.getType() ).thenReturn( Integer.MAX_VALUE );
    resultSetMetaData.getColumnType( 1 );
  }
}
