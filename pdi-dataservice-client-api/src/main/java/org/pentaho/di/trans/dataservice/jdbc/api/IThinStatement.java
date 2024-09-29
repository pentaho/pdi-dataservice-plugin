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
