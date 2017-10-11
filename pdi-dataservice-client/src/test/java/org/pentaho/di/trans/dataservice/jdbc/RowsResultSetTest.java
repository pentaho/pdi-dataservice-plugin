/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.util.concurrent.ThreadLocalRandom;

import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.containsStringIgnoringCase;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * @author nhudak
 */
@RunWith( MockitoJUnitRunner.class )
public class RowsResultSetTest extends BaseResultSetTest {
  RowsResultSet resultSet;

  public RowsResultSetTest() {
    super( RowsResultSet.class );
  }

  @Before
  public void setUp() throws Exception {
    resultSet = new RowsResultSet( rowMeta, ImmutableList.<Object[]>of() );
  }

  @Override void setNextRow( Object[] nextRow ) {
    resultSet = new RowsResultSet( rowMeta, ImmutableList.of( nextRow ) );
  }

  @Override protected RowsResultSet getTestObject() {
    return resultSet;
  }

  @Test
  public void testRowScrolling() throws Exception {
    Object[] r1 = new Object[0];
    Object[] r2 = new Object[0];
    Object[] r3 = new Object[0];
    resultSet = new RowsResultSet( rowMeta, ImmutableList.of( r1, r2, r3 ) );

    assertThat( resultSet.getType(), is( ResultSet.TYPE_SCROLL_INSENSITIVE ) );
    assertThat( resultSet.getFetchDirection(), anything() );

    verifyState( "beforeFirst" );
    assertThat( resultSet.first(), is( true ) );
    verifyState( "first" );
    assertThat( resultSet.getRow(), is( 1 ) );

    assertThat( resultSet.next(), is( true ) );
    assertThat( resultSet.getRow(), is( 2 ) );
    verifyState();

    assertThat( resultSet.next(), is( true ) );
    assertThat( resultSet.getRow(), is( 3 ) );
    verifyState( "last" );

    assertThat( resultSet.next(), is( false ) );
    verifyState( "afterLast" );

    assertThat( resultSet.previous(), is( true ) );
    verifyState( "last" );
  }

  @Test
  public void testEmptyResultSet() throws Exception {
    verifyState( "beforeFirst", "afterLast" );
    assertThat( resultSet.next(), is( false ) );
    verifyState( "afterLast" );

    assertThat( resultSet.next(), is( false ) );
    verifyState( "afterLast" );
    verifyNoMoreInteractions( rowMeta );
  }

  @Test
  public void testClose() throws Exception {
    assertThat( resultSet.isClosed(), is( false ) );
    verifyState( "beforeFirst", "afterLast" );

    resultSet.close();
    resultSet.close();

    assertThat( resultSet.isClosed(), is( true ) );

    for ( Method method : STATES.values() ) {
      try {
        assertThat( invoke( resultSet, method ), not( anything() ) );
      } catch ( InvocationTargetException e ) {
        assertThat( e.getCause().getMessage(), containsStringIgnoringCase( "closed" ) );
      }
    }
  }

  @Test
  public void testProperties() throws Exception {
    resultSet.setFetchSize( ThreadLocalRandom.current().nextInt() );
    assertThat( resultSet.getFetchSize(), is( 0 ) );

    assertThat( resultSet.getConcurrency(), is( ResultSet.CONCUR_READ_ONLY ) );
    assertThat( resultSet.getHoldability(), anything() );
    assertThat( resultSet.getCursorName(), anything() );
  }
}
