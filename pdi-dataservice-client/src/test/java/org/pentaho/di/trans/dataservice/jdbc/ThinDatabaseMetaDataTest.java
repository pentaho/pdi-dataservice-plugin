package org.pentaho.di.trans.dataservice.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.trans.dataservice.client.DataServiceClientService;

import java.lang.reflect.Method;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

/**
 * @author nhudak
 */
@RunWith( MockitoJUnitRunner.class )
public class ThinDatabaseMetaDataTest extends JDBCTestBase<ThinDatabaseMetaData> {

  @Mock ThinConnection connection;
  @Mock DataServiceClientService clientService;
  @InjectMocks ThinDatabaseMetaData metaData;

  @Before
  public void setUp() throws Exception {
    when( connection.getClientService() ).thenReturn( clientService );

    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "valuename" ) );
    when( clientService.getServiceInformation() ).thenReturn( ImmutableList.of(
      new ThinServiceInformation( "sequence", rowMeta )
    ) );
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
        assertThat( invoke( metaData, method ), allOf(
            hasProperty( "beforeFirst", is( true ) ),
            hasProperty( "last", is( true ) ) )
        );
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

  @Override protected ThinDatabaseMetaData getTestObject() {
    return metaData;
  }
}
