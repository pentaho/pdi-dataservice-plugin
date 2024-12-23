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


package org.pentaho.di.trans.dataservice.client;

import org.pentaho.di.core.ProvidesDatabaseConnectionInformation;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.dataservice.jdbc.ThinConnection;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.net.URI;
import java.net.URISyntaxException;

public class DataServiceConnectionInformation implements ProvidesDatabaseConnectionInformation {
  public static final String KETTLE_THIN = "KettleThin";
  public static final String NATIVE = "native";
  private String dataServiceName;
  private Repository repository;
  private LogChannelInterface log;
  private static Class<?> PKG = DataServiceConnectionInformation.class; // for i18n purposes, needed by Translator2!!

  public DataServiceConnectionInformation( String dataService, Repository repository, final LogChannelInterface log ) {
    this.dataServiceName = dataService;
    this.repository = repository;
    this.log = log;
  }

  @Override public DatabaseMeta getDatabaseMeta() {
    try {
      return isDIRepository() ? getRepositoryDatabaseMeta() : getLocalDatabaseMeta();
    } catch ( KettleException e ) {
      log.logDebug( e.getMessage() );
      return getLocalDatabaseMeta();
    }
  }

  private boolean isDIRepository() {
    try {
      return repository != null && "PentahoEnterpriseRepository".equals( getRepositoryProperty( "id" ) );
    } catch ( KettleException e ) {
      log.logDebug( e.getMessage() );
      return false;
    }
  }

  private DatabaseMeta getRepositoryDatabaseMeta() throws KettleException {
    String repositoryUrl = getRepositoryProperty( "repository_location_url" );
    try {
      URI uri = new URI( repositoryUrl );
      DatabaseMeta databaseMeta =
        new DatabaseMeta( dataServiceName, KETTLE_THIN, NATIVE, uri.getHost(), KETTLE_THIN,
          Integer.toString( uri.getPort() ), repository.getUserInfo().getLogin(),
          repository.getUserInfo().getPassword() );
      databaseMeta.setDBName( uri.getPath().substring( 1 ) );
      if ( uri.getScheme() != null && uri.getScheme().equalsIgnoreCase( "https" ) ) {
        databaseMeta.addExtraOption( databaseMeta.getPluginId(), ThinConnection.ARG_ISSECURE, "true" );
      }
      return databaseMeta;
    } catch ( URISyntaxException e ) {
      throw new KettleException( e );
    }
  }

  private String getRepositoryProperty( final String nodeName ) throws KettleException {
    final String xml = repository.getRepositoryMeta().getXML();
    final Document doc = XMLHandler.loadXMLString( xml );
    final NodeList childNodes = doc.getDocumentElement().getChildNodes();
    for ( int i = 0; i < childNodes.getLength(); i++ ) {
      final Node item = childNodes.item( i );
      if ( nodeName.equals( item.getNodeName() ) ) {
        return item.getTextContent();
      }
    }
    throw new KettleException( BaseMessages.getString( PKG, "BuildModelJob.Info.ElementNotFound", nodeName ) );
  }

  private DatabaseMeta getLocalDatabaseMeta() {
    DatabaseMeta databaseMeta =
      new DatabaseMeta( dataServiceName, KETTLE_THIN, NATIVE, null, KETTLE_THIN, null, null, null );
    databaseMeta.addExtraOption( KETTLE_THIN, "local", "true" );
    return databaseMeta;
  }

  @Override public String getTableName() {
    return dataServiceName;
  }

  @Override public String getSchemaName() {
    return "Kettle";
  }

  @Override public String getMissingDatabaseConnectionInformationMessage() {
    // Use default connection missing message
    return null;
  }
}
