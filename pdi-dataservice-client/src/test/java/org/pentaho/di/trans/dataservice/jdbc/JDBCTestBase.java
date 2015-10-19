/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

import com.google.common.collect.ImmutableSet;
import org.junit.Test;
import org.pentaho.di.trans.dataservice.jdbc.annotation.NotSupported;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Wrapper;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/**
 * @author nhudak
 */
public abstract class JDBCTestBase<C> {

  protected final Class<C> type;

  public JDBCTestBase( Class<C> type ) {
    this.type = type;
  }

  protected abstract C getTestObject();

  protected Set<Class<?>> getSupportedWrappers() {
    return ImmutableSet.of();
  }

  @Test
  public void testUnsupportedMethods() {
    Object object = getTestObject();
    for ( Method method : type.getMethods() ) {
      if ( method.getAnnotation( NotSupported.class ) != null ) {
        try {
          invoke( object, method );
          fail( "Expected SQLFeatureNotSupportedException from " + method );
        } catch ( InvocationTargetException e ) {
          assertThat( method.getName(), e.getCause(), instanceOf( SQLFeatureNotSupportedException.class ) );
        } catch ( Exception e ) {
          throw new AssertionError( "Error executing " + method, e );
        }
      }
    }
  }

  @Test
  public void testUnwrap() throws Exception {
    C object = getTestObject();
    if ( object instanceof Wrapper ) {
      Wrapper wrapper = (Wrapper) object;
      for ( Class<?> iFace : getSupportedWrappers() ) {
        assertThat( wrapper.isWrapperFor( iFace ), is( true ) );
        assertThat( wrapper.unwrap( iFace ), instanceOf( iFace ) );
      }
      assertThat( wrapper.isWrapperFor( SecretInterface.class ), is( false ) );
      try {
        fail( "Unexpected result from unwrap(SecretInterface)" + wrapper.unwrap( SecretInterface.class ) );
      } catch ( SQLException ignored ) {
        // SQL Exception expected
      }
    }
  }

  private interface SecretInterface {
  }

  protected Object invoke( Object object, Method method ) throws IllegalAccessException, InvocationTargetException {
    return method.invoke( object, mockArguments( method ) );
  }

  protected Object[] mockArguments( Method method ) {
    Class<?>[] parameterTypes = method.getParameterTypes();
    Object[] args = new Object[parameterTypes.length];
    for ( int i = 0; i < parameterTypes.length; i++ ) {
      args[i] = mockValue( parameterTypes[i] );
    }
    return args;
  }

  protected Object mockValue( Class<?> type ) {
    Object value;
    if ( type.equals( String.class ) ) {
      value = UUID.randomUUID().toString();
    } else if ( type.equals( Boolean.TYPE ) ) {
      value = true;
    } else if ( type.equals( Integer.TYPE ) ) {
      value = new Random().nextInt();
    } else if ( type.equals( Class.class ) ) {
      value = Object.class;
    } else if ( type.isArray() ) {
      value = Array.newInstance( type.getComponentType(), 0 );
    } else {
      value = mock( type );
    }
    return value;
  }
}
