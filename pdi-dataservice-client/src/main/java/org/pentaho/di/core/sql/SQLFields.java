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

package org.pentaho.di.core.sql;

import com.google.common.collect.Lists;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleSQLException;
import org.pentaho.di.core.jdbc.ThinUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;

import java.util.ArrayList;
import java.util.List;

public class SQLFields {
  private static final String DISTINCT_PREFIX = "DISTINCT ";
  private String tableAlias;
  private RowMetaInterface serviceFields;
  private String fieldsClause;
  private List<SQLField> fields;
  private SQLFields selectFields;

  private boolean distinct;

  public SQLFields( String tableAlias, RowMetaInterface serviceFields, String fieldsClause ) throws KettleSQLException {
    this( tableAlias, serviceFields, fieldsClause, false );
  }

  public SQLFields( String tableAlias, RowMetaInterface serviceFields, String fieldsClause, boolean orderClause ) throws KettleSQLException {
    this( tableAlias, serviceFields, fieldsClause, orderClause, null );
  }

  public SQLFields( String tableAlias, RowMetaInterface serviceFields, String fieldsClause, boolean orderClause,
    SQLFields selectFields ) throws KettleSQLException {
    this.tableAlias = tableAlias;
    this.serviceFields = serviceFields;
    this.fieldsClause = fieldsClause;
    this.selectFields = selectFields;
    fields = Lists.newArrayList();

    distinct = false;

    parse( orderClause );
  }

  private void parse( boolean orderClause ) throws KettleSQLException {
    if ( Const.isEmpty( fieldsClause ) ) {
      return;
    }

    if ( fieldsClause.regionMatches( true, 0, DISTINCT_PREFIX, 0, DISTINCT_PREFIX.length() ) ) {
      distinct = true;
      fieldsClause = fieldsClause.substring( DISTINCT_PREFIX.length() );
    }

    List<String> strings = new ArrayList<String>();
    int startIndex = 0;
    for ( int index = 0; index < fieldsClause.length(); index++ ) {
      index = ThinUtil.skipChars( fieldsClause, index, '"', '\'', '(' );
      if ( index >= fieldsClause.length() ) {
        strings.add( fieldsClause.substring( startIndex ) );
        startIndex = -1;
        break;
      }
      if ( fieldsClause.charAt( index ) == ',' ) {
        strings.add( fieldsClause.substring( startIndex, index ) );
        startIndex = index + 1;
      }
    }
    if ( startIndex >= 0 ) {
      strings.add( fieldsClause.substring( startIndex ) );
    }

    // Now grab all the fields and determine their composition...
    //
    fields.clear();
    for ( String string : strings ) {
      String fieldString = ThinUtil.stripTableAlias( Const.trim( string ), tableAlias );
      if ( "*".equals( fieldString ) ) {
        // Add all service fields
        //
        for ( ValueMetaInterface valueMeta : serviceFields.getValueMetaList() ) {
          fields.add( new SQLField(
            tableAlias, "\"" + valueMeta.getName() + "\"", serviceFields, orderClause, selectFields ) );
        }
      } else {
        fields.add( new SQLField( tableAlias, fieldString, serviceFields, orderClause, selectFields ) );
      }
    }

    indexFields();
  }

  public List<SQLField> getFields() {
    return fields;
  }

  public List<SQLField> getNonAggregateFields() {
    List<SQLField> list = new ArrayList<SQLField>();
    for ( SQLField field : fields ) {
      if ( field.getAggregation() == null ) {
        list.add( field );
      }
    }
    return list;
  }

  public List<SQLField> getAggregateFields() {
    List<SQLField> list = new ArrayList<SQLField>();
    for ( SQLField field : fields ) {
      if ( field.getAggregation() != null ) {
        list.add( field );
      }
    }
    return list;
  }

  public boolean isEmpty() {
    return fields.isEmpty();
  }

  /**
   * Find a field by it's field name (not alias)
   *
   * @param fieldName
   *          the name of the field
   * @return the field or null if nothing was found.
   */
  public SQLField findByName( String fieldName ) {
    for ( SQLField field : fields ) {
      if ( field.getField().equalsIgnoreCase( fieldName ) ) {
        return field;
      }
    }
    return null;
  }

  /**
   * @return the serviceFields
   */
  public RowMetaInterface getServiceFields() {
    return serviceFields;
  }

  /**
   * @param serviceFields
   *          the serviceFields to set
   */
  public void setServiceFields( RowMetaInterface serviceFields ) {
    this.serviceFields = serviceFields;
  }

  /**
   * @return the fieldsClause
   */
  public String getFieldsClause() {
    return fieldsClause;
  }

  /**
   * @param fieldsClause
   *          the fieldsClause to set
   */
  public void setFieldsClause( String fieldsClause ) {
    this.fieldsClause = fieldsClause;
  }

  /**
   * @return the selectFields
   */
  public SQLFields getSelectFields() {
    return selectFields;
  }

  /**
   * @param selectFields
   *          the selectFields to set
   */
  public void setSelectFields( SQLFields selectFields ) {
    this.selectFields = selectFields;
  }

  /**
   * @param fields
   *          the fields to set
   */
  public void setFields( List<SQLField> fields ) {
    this.fields = fields;
    indexFields();
  }

  private void indexFields() {
    for ( int i = 0; i < fields.size(); i++ ) {
      fields.get( i ).setFieldIndex( i );
    }
  }

  /**
   * @return true if one or more fields is an aggregation.
   */
  public boolean hasAggregates() {
    for ( SQLField field : fields ) {
      if ( field.getAggregation() != null ) {
        return true;
      }
    }
    return false;
  }

  public List<SQLField> getIifFunctionFields() {
    List<SQLField> list = new ArrayList<SQLField>();

    for ( SQLField field : fields ) {
      if ( field.getIif() != null ) {
        list.add( field );
      }
    }

    return list;
  }

  public List<SQLField> getRegularFields() {
    List<SQLField> list = new ArrayList<SQLField>();

    for ( SQLField field : fields ) {
      if ( field.getIif() == null && field.getAggregation() == null && field.getValueData() == null ) {
        list.add( field );
      }
    }

    return list;
  }

  public List<SQLField> getConstantFields() {
    List<SQLField> list = new ArrayList<SQLField>();

    for ( SQLField field : fields ) {
      if ( field.getValueMeta() != null && field.getValueData() != null ) {
        list.add( field );
      }
    }

    return list;
  }

  /**
   * @return the distinct
   */
  public boolean isDistinct() {
    return distinct;
  }

  /**
   * @return the tableAlias
   */
  public String getTableAlias() {
    return tableAlias;
  }

}
