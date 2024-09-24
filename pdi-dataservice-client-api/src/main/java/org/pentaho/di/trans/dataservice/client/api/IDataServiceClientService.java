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

package org.pentaho.di.trans.dataservice.client.api;

import io.reactivex.Observer;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.dataservice.jdbc.api.IThinServiceInformation;
import org.pentaho.metastore.api.IMetaStore;

import java.io.DataInputStream;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface IDataServiceClientService {
  String PARAMETER_PREFIX = "PARAMETER_";
  enum StreamingMode {
    TIME_BASED, ROW_BASED
  }

  void setMetaStore( IMetaStore metaStore );
  void setRepository( Repository repository );
  DataInputStream query( String sql, int maxRows ) throws SQLException;
  DataInputStream query( String sql, int maxRows, Map<String, String> params  ) throws SQLException;
  DataInputStream query( String sql, StreamingMode windowMode, long windowSize, long windowEvery,
                         long windowLimit ) throws SQLException;
  DataInputStream query( String sql, StreamingMode windowMode, long windowSize, long windowEvery,
                         long windowLimit, Map<String, String> params ) throws SQLException;
  List<IThinServiceInformation> getServiceInformation() throws SQLException;
  IThinServiceInformation getServiceInformation( String name ) throws SQLException;
  List<String> getServiceNames() throws SQLException;
  List<String> getServiceNames( String serviceName ) throws SQLException;

  /**
   * Execute query pushing results.
   * @param sql query to execute
   * @param streamParams streaming window parameters
   * @param params parameters map
   * @param consumer where to push the results
   * @throws Exception
   */
  void query( String sql,
              IStreamingParams streamParams,
              Map<String, String> params,
              Observer<List<RowMetaAndData>> consumer ) throws Exception;

  interface IStreamingParams {
    IDataServiceClientService.StreamingMode getWindowMode();
    long getWindowSize();
    long getWindowEvery();
    long getWindowLimit();
  }
}
