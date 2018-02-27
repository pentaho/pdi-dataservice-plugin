/*! ******************************************************************************
 *
 * Pentaho Community Edition Project: data-refinery-pdi-plugin
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
 *
 * *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ********************************************************************************/

package org.pentaho.di.trans.dataservice.client;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseInterface;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.plugins.DatabasePluginType;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.PluginTypeInterface;
import org.pentaho.di.repository.IUser;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryMeta;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class DataServiceConnectionInformationTest {
  private static PluginInterface mockDbPlugin;
  private LogChannelInterface log;

  @Before
  public void setUp() throws Exception {
    log = mock( LogChannelInterface.class );
  }

  @After
  public void tearDown() throws Exception {
    verifyNoMoreInteractions( log );
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    if ( !KettleEnvironment.isInitialized() ) {
      KettleEnvironment.init();
    }
    Map<Class<?>, String> dbMap = new HashMap<Class<?>, String>();
    dbMap.put( DatabaseInterface.class, DataServiceClientPlugin.class.getName() );
    PluginRegistry preg = PluginRegistry.getInstance();
    mockDbPlugin = mock( PluginInterface.class );
    when( mockDbPlugin.matches( anyString() ) ).thenReturn( true );
    when( mockDbPlugin.isNativePlugin() ).thenReturn( true );
    when( mockDbPlugin.getMainType() ).thenAnswer( new Answer<Class<?>>() {
      @Override
      public Class<?> answer( InvocationOnMock invocation ) throws Throwable {
        return DatabaseInterface.class;
      }
    } );

    when( mockDbPlugin.getPluginType() ).thenAnswer( new Answer<Class<? extends PluginTypeInterface>>() {
      @Override
      public Class<? extends PluginTypeInterface> answer( InvocationOnMock invocation ) throws Throwable {
        return DatabasePluginType.class;
      }
    } );

    when( mockDbPlugin.getIds() ).thenReturn( new String[] { "KettleThin" } );
    when( mockDbPlugin.getName() ).thenReturn( "KettleThin" );
    when( mockDbPlugin.getClassMap() ).thenReturn( dbMap );

    preg.registerPlugin( DatabasePluginType.class, mockDbPlugin );
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    PluginRegistry.getInstance().removePlugin( DatabasePluginType.class, mockDbPlugin );
  }

  @Test
  public void testNullRepositoriesReturnsLocalMeta() throws Exception {
    String dataServiceName = "myDataService";
    DataServiceConnectionInformation connectInfo = new DataServiceConnectionInformation( dataServiceName, null, log );
    assertLocalConnection( dataServiceName, connectInfo );
  }

  @Test
  public void testDIRepositoriesReturnsMetaToSameHost() throws Exception {
    String dataServiceName = "myDataService";
    final Repository repository = expectRepository(
      "  <repository>"
        + "  <id>PentahoEnterpriseRepository</id>\n"
        + "  <name>local</name>\n"
        + "  <description>a</description>\n"
        + "  <repository_location_url>http&#x3a;&#x2f;&#x2f;farfaraway&#x3a;12345&#x2f;pentaho</repository_location_url>\n"
        + "  <version_comment_mandatory>N</version_comment_mandatory>\n"
        + "</repository>"
    );
    final IUser user = mock( IUser.class );
    when( repository.getUserInfo() ).thenReturn( user );
    when( user.getLogin() ).thenReturn( "open" );
    when( user.getPassword() ).thenReturn( "sesame" );
    DataServiceConnectionInformation connectInfo = new DataServiceConnectionInformation( dataServiceName, repository,
      log );
    assertEquals( dataServiceName, connectInfo.getTableName() );
    assertEquals( "Kettle", connectInfo.getSchemaName() );
    DatabaseMeta databaseMeta = connectInfo.getDatabaseMeta();
    assertEquals( "org.pentaho.di.trans.dataservice.jdbc.ThinDriver", databaseMeta.getDriverClass() );
    assertEquals( "pentaho", databaseMeta.getDatabaseName() );
    assertEquals( dataServiceName, databaseMeta.getName() );
    assertNull( databaseMeta.getExtraOptions().get( "KettleThin.local" ) );
    assertEquals( "farfaraway", databaseMeta.getHostname() );
    assertEquals( "12345", databaseMeta.getDatabasePortNumberString() );
    assertEquals( "open", databaseMeta.getUsername() );
    assertEquals( "sesame", databaseMeta.getPassword() );
  }

  @Test
  public void testOtherRepositoryTypesReturnLocalMeta() throws Exception {
    String dataServiceName = "yourDataService";
    final Repository repository = expectRepository(
      "<repository>"
      + "  <id>KettleFileRepository</id>\n"
      + "  <name>local</name>\n"
      + "  <description>a</description>\n"
      + "  <repository_location_url>http&#x3a;&#x2f;&#x2f;farfaraway&#x3a;12345&#x2f;pentaho</repository_location_url>\n"
      + "  <version_comment_mandatory>N</version_comment_mandatory>\n"
      + "</repository>" );
    DataServiceConnectionInformation connectInfo = new DataServiceConnectionInformation( dataServiceName, repository,
      log );
    assertLocalConnection( dataServiceName, connectInfo );
  }

  @Test
  public void testWrongTagLogsInfoAndReturnsLocalMeta() throws Exception {
    String dataServiceName = "otherDataService";
    Repository repository = expectRepository(
      "<repository>"
      + "  <id>PentahoEnterpriseRepository</id>\n"
      + "  <name>local</name>\n"
      + "  <description>a</description>\n"
      + "  <repository_location>http&#x3a;&#x2f;&#x2f;farfaraway&#x3a;12345&#x2f;pentaho</repository_location>\n"
      + "  <version_comment_mandatory>N</version_comment_mandatory>\n"
      + "</repository>" );
    DataServiceConnectionInformation connectInfo =
      new DataServiceConnectionInformation( dataServiceName, repository, log );
    assertLocalConnection( dataServiceName, connectInfo );
    verify( log ).logDebug( Mockito.contains( "Element repository_location_url not found in Repository Meta XML" ) );
  }

  @Test
  public void testBadXmlLogsInfoAndReturnsLocalMeta() throws Exception {
    String dataServiceName = "otherDataService";
    Repository repository = expectRepository(
      "<reposito></repository>" );
    DataServiceConnectionInformation connectInfo =
      new DataServiceConnectionInformation( dataServiceName, repository, log );
    assertLocalConnection( dataServiceName, connectInfo );
    verify( log ).logDebug( Mockito.contains( "Error reading information from XML string" ) );
  }

  @Test
  public void testNoSpecificMessageForMissingParameters() throws Exception {
    DataServiceConnectionInformation connectionInformation =
      new DataServiceConnectionInformation( "", null, log );
    assertNull( connectionInformation.getMissingDatabaseConnectionInformationMessage() );
  }

  private Repository expectRepository( final String xml ) {
    final Repository repository = mock( Repository.class );
    final RepositoryMeta repositoryMeta = mock( RepositoryMeta.class );
    when( repository.getRepositoryMeta() ).thenReturn( repositoryMeta );
    when( repositoryMeta.getXML() ).thenReturn( xml );
    return repository;
  }

  private void assertLocalConnection( final String dataServiceName, final DataServiceConnectionInformation connectInfo ) {
    assertEquals( dataServiceName, connectInfo.getTableName() );
    assertEquals( "Kettle", connectInfo.getSchemaName() );
    DatabaseMeta databaseMeta = connectInfo.getDatabaseMeta();
    assertEquals( "org.pentaho.di.trans.dataservice.jdbc.ThinDriver", databaseMeta.getDriverClass() );
    assertEquals( "KettleThin", databaseMeta.getDatabaseName() );
    assertEquals( dataServiceName, databaseMeta.getName() );
    assertEquals( "true", databaseMeta.getExtraOptions().get( "KettleThin.local" ) );
  }
}
