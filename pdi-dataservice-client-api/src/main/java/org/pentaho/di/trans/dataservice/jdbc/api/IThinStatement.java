/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.trans.dataservice.jdbc.api;

import io.reactivex.Observer;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService;
import org.pentaho.di.trans.dataservice.client.api.IDataServiceClientService.IStreamingParams;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Data Services Thin Statement Interface.
 */
public interface IThinStatement extends Statement {
  /**
   * Executes a query with the given size and rate. If the windowRowSize > 0 and windowMillisSize = 0 then
   * a row based window is created with the given windowRowSize and windowRate will be considered in rows.
   * If windowRowSize = 0 and windowMillisSize > 0 then a time based window is created with the given windowMillisSize
   * in milliseconds and windowRate will be considered in milliseconds.
   * If windowRowSize > 0 and windowMillisSize > 0 then what occurs first is considered, time or rows, and windowRate
   * is discarded.
   *
   * @param sql The sql query.
   * @param windowMode The streaming window mode.
   * @param windowSize The query window size. Number of rows for a ROW_BASED streamingType and milliseconds for a
   *                 TIME_BASED streamingType.
   * @param windowEvery The query window rate. Number of rows for a ROW_BASED streamingType and milliseconds for a
   *                 TIME_BASED streamingType.
   * @param windowLimit The query window limit. Number of rows for a TIME_BASED streamingType and milliseconds for a
   *                 ROW_BASED streamingType.
   * @return The query ResultSet.
   */
  ResultSet executeQuery(  String sql, IDataServiceClientService.StreamingMode windowMode,
                           long windowSize, long windowEvery,
                           long windowLimit  ) throws SQLException;

  /**
   * Same as {@link #executeQuery(String, IDataServiceClientService.StreamingMode, long, long, long)}
   * but results are pushed from the observable. <b>Only available in local mode.</b>
   * @param sql The sql query.
   * @param params params Streaming window parameters.
   * @param consumer the consumer for the rows.
   * @throws Exception
   */
  void executePushQuery( String sql, IStreamingParams params, Observer<List<RowMetaAndData>> consumer ) throws Exception;

}
