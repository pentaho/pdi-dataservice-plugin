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

package org.pentaho.di.trans.dataservice.client;

import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import java.lang.reflect.Method;
import java.net.URL;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.database.DatabaseInterface;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.plugins.DatabaseMetaPlugin;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.dataservice.jdbc.ThinConnection;
import org.pentaho.di.trans.dataservice.jdbc.ThinDriver;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

/**
 * @author nhudak
 */
@RunWith( MockitoJUnitRunner.class )
public class DataServiceClientPluginTest {

  DataServiceClientPlugin clientPlugin;

  @Before
  public void setUp() throws Exception {
    clientPlugin = new DataServiceClientPlugin();

    DatabaseMetaPlugin annotation = DataServiceClientPlugin.class.getAnnotation( DatabaseMetaPlugin.class );
    clientPlugin.setPluginId( annotation.type() );
    clientPlugin.setPluginName( annotation.typeDescription() );
  }

  @Test
  public void testSupportChecks() throws Exception {
    ImmutableSet<String> supported = ImmutableSet.of(
      "supportsBooleanDataType", "supportsOptionsInURL", "supportsErrorHandling", "supportsSetMaxRows",
      "supportsSetLong", "supportsTimeStampToDateConversion"
    );

    for ( Method method : DatabaseInterface.class.getMethods() ) {
      String methodName = method.getName();
      if ( methodName.startsWith( "supports" ) && method.getReturnType() == Boolean.TYPE ) {
        assertThat( methodName, (Boolean) method.invoke( clientPlugin ), is( supported.contains( methodName ) ) );
      }
    }

    assertThat( clientPlugin.isFetchSizeSupported(), is( false ) );
  }

  @Test
  public void testDDL() throws Exception {
    assertThat( clientPlugin.getCreateTableStatement(), is( "// Unsupported" ) );
    assertThat( clientPlugin.getTruncateTableStatement( "dataService" ), is( "// Unsupported" ) );

    assertThat(
      clientPlugin.getAddColumnStatement( "dataService", mock( ValueMetaInterface.class ), "", false, "", false ),
      is( "// Unsupported" ) );
    assertThat(
      clientPlugin.getModifyColumnStatement( "dataService", mock( ValueMetaInterface.class ), "", false, "", false ),
      is( "// Unsupported" ) );
    assertThat(
      clientPlugin.getDropColumnStatement( "dataService", mock( ValueMetaInterface.class ), "", false, "", false ),
      is( "// Unsupported" ) );

    assertThat(
      clientPlugin.getFieldDefinition( mock( ValueMetaInterface.class ), "", "", false, false, false ),
      is( "// Unsupported" ) );
  }

  @Test
  public void testDriverProperties() throws Exception {
    assertThat( Ints.asList( clientPlugin.getAccessTypeList() ), containsInAnyOrder(
      DatabaseMeta.TYPE_ACCESS_NATIVE, DatabaseMeta.TYPE_ACCESS_JNDI
    ) );

    assertThat( clientPlugin.getUsedLibraries(), emptyArray() );

    assertThat( clientPlugin.getDefaultDatabasePort(), is( 8080 ) );
    assertThat( clientPlugin.getDriverClass(), is( ThinDriver.class.getName() ) );
    assertThat( new URL( clientPlugin.getExtraOptionsHelpText() ), notNullValue() );

    clientPlugin.setDatabaseName( "override" );
    assertThat( clientPlugin.getDatabaseName(), is( "override" ) );

    assertThat( clientPlugin.getExtraOptionIndicator(), is( "?" ) );
    assertThat( clientPlugin.getExtraOptionSeparator(), is( "&" ) );
  }

  @Test
  public void testConstructURL() throws Exception {
    assertThat( clientPlugin.getURL( "host.com", "8080", "pentaho" ), is( "jdbc:pdi://host.com:8080/pentaho/kettle" ) );
    clientPlugin.getAttributes().put( ThinConnection.ARG_WEB_APPLICATION_NAME, "pentaho" );
    assertThat( clientPlugin.getURL( "host.com", "8080", "kettle" ), is( "jdbc:pdi://host.com:8080/kettle" ) );
  }

  @Test
  public void testRequiresName() {
    assertFalse( clientPlugin.requiresName() );
  }
}
