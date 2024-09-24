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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;

import java.lang.reflect.Method;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class ThinDatabaseMetaDataTest extends JDBCTestBase<ThinDatabaseMetaData> {

  @Mock ThinConnection connection;
  @Mock IDataServiceClientService clientService;
  @InjectMocks ThinDatabaseMetaData metaData;

  @Before
  public void setUp() throws Exception {
    when( connection.getClientService() ).thenReturn( clientService );

    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "valuename" ) );
    when( clientService.getServiceInformation( anyString() ) ).thenReturn(
      new ThinServiceInformation( "sequence", false, rowMeta )
    );
    when( clientService.getServiceInformation() ).thenReturn(
      ImmutableList.of( new ThinServiceInformation( "sequence", false, rowMeta ) )
    );
    when( clientService.getServiceNames( anyString() ) ).thenReturn(
      ImmutableList.of( "sequence" )
    );
  }

  public ThinDatabaseMetaDataTest() {
    super( ThinDatabaseMetaData.class );
  }

  @Test
  public void testGetServiceInformation() throws Exception {
    ResultSet schemas = metaData.getSchemas();
    assertThat( schemas.next(), is( true ) );
    assertThat( schemas.getString( "TABLE_SCHEM" ), equalTo( "Kettle" ) );
    assertThat( schemas.next(), is( false ) );

    ResultSet tables = metaData.getTables( null, null, null, null );
    assertThat( tables.next(), is( true ) );
    assertThat( tables.getString( "TABLE_NAME" ), equalTo( "sequence" ) );
    assertThat( tables.next(), is( false ) );

    ResultSet columns = metaData.getColumns( null, null, null, null );
    assertThat( columns.next(), is( true ) );
    assertThat( columns.getString( "TABLE_NAME" ), equalTo( "sequence" ) );
    assertThat( columns.getString( "COLUMN_NAME" ), equalTo( "valuename" ) );
    assertThat( columns.next(), is( false ) );

    tables = metaData.getTables( null, null, "sequence", null );
    assertThat( tables.next(), is( true ) );
    assertThat( tables.getString( "TABLE_NAME" ), equalTo( "sequence" ) );
    assertThat( tables.next(), is( false ) );

    columns = metaData.getColumns( null, null, "sequence", null );
    assertThat( columns.next(), is( true ) );
    assertThat( columns.getString( "TABLE_NAME" ), equalTo( "sequence" ) );
    assertThat( columns.getString( "COLUMN_NAME" ), equalTo( "valuename" ) );
    assertThat( columns.next(), is( false ) );
  }

  @Test
  public void testEmptyResultSets() throws Exception {
    ImmutableSet<String> emptySetMethods = ImmutableSet.of(
      "getAttributes", "getBestRowIdentifier", "getCatalogs", "getClientInfoProperties", "getColumnPrivileges",
      "getCrossReference", "getExportedKeys", "getFunctionColumns", "getFunctions", "getImportedKeys",
      "getIndexInfo", "getPrimaryKeys", "getProcedureColumns", "getProcedures",
      "getSuperTables", "getSuperTypes", "getTablePrivileges", "getTableTypes", "getTypeInfo", "getUDTs",
      "getVersionColumns"
    );

    for ( Method method : DatabaseMetaData.class.getMethods() ) {
      if ( emptySetMethods.contains( method.getName() ) ) {
        assertThat( invoke( metaData, method ), hasProperty( "afterLast", is( true ) ) );
      }
    }
  }

  @Test
  public void testSupportChecks() throws Exception {
    ImmutableSet<String> supportedFeatures = ImmutableSet.of(
      "allTablesAreSelectable", "isReadOnly", "nullPlusNonNullIsNull", "nullsAreSortedAtStart", "nullsAreSortedLow",
      "supportsColumnAliasing", "supportsGroupBy", "supportsMinimumSQLGrammar", "supportsMixedCaseIdentifiers",
      "supportsMixedCaseQuotedIdentifiers", "supportsMultipleOpenResults", "supportsNonNullableColumns",
      "supportsOrderByUnrelated", "isWrapperFor"
    );

    for ( Method method : DatabaseMetaData.class.getMethods() ) {
      if ( method.getReturnType().equals( Boolean.TYPE ) ) {
        Boolean supported = supportedFeatures.contains( method.getName() );
        assertThat( method.toString(), (Boolean) invoke( metaData, method ), equalTo( supported ) );
      }
    }
  }

  @Test
  public void testGetColumnsWithSQLExpressionAsTableNamePattern() throws Exception {
    String tableName = "dataServiceTable";
    String sql = "select * from " + tableName;
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "valuename" ) );
    when( clientService.getServiceNames( tableName ) ).thenReturn(
      Arrays.asList( tableName ) );
    when( clientService.getServiceInformation( tableName ) ).thenReturn(
      new ThinServiceInformation( tableName, false, rowMeta )
    );

    ResultSet columns = metaData.getColumns( null, null, sql, null );
    assertThat( columns.next(), is( true ) );
    assertThat( columns.getString( "TABLE_NAME" ), equalTo( tableName ) );
  }

  @Test
  public void testGetColumnsTableNamePattern() throws Exception {
    String tableName = "dataServiceTable";
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "valuename" ) );
    when( clientService.getServiceNames( tableName ) ).thenReturn(
      Arrays.asList( tableName ) );
    when( clientService.getServiceInformation( tableName ) ).thenReturn(
      new ThinServiceInformation( tableName, false, rowMeta )
    );

    ResultSet columns = metaData.getColumns( null, null, tableName, null );
    assertThat( columns.next(), is( true ) );
    assertThat( columns.getString( "TABLE_NAME" ), equalTo( tableName ) );
  }

  @Override protected ThinDatabaseMetaData getTestObject() {
    return metaData;
  }
}
