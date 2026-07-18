/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/



package org.pentaho.di.trans.dataservice.jdbc.api;

import org.pentaho.di.core.row.RowMetaInterface;

public interface IThinServiceInformation {
  String getName();
  boolean isStreaming();
  RowMetaInterface getServiceFields();
}
