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

import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.value.ValueMetaString;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author nhudak
 */
class MockDataInput extends DataOutputStream {
  public MockDataInput() {
    super( new ByteArrayOutputStream() );
  }

  public static MockDataInput dual() throws IOException, KettleFileException {
    MockDataInput dataOutput = new MockDataInput();
    dataOutput.writeUTF( "dual" );
    dataOutput.writeUTF( "" );
    dataOutput.writeUTF( "" );
    dataOutput.writeUTF( "" );
    dataOutput.writeUTF( "" );

    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "DUMMY" ) );
    rowMeta.writeMeta( dataOutput );
    rowMeta.writeData( dataOutput, new Object[] { "x" } );
    return dataOutput;
  }

  public static MockDataInput errors() throws IOException, KettleFileException {
    MockDataInput dataOuput = new MockDataInput();
    dataOuput.writeUTF( "false" );

    return dataOuput;
  }

  public static MockDataInput stop() throws IOException, KettleFileException {
    MockDataInput dataOuput = new MockDataInput();
    dataOuput.writeUTF( "true" );

    return dataOuput;
  }

  public byte[] getBuffer() {
    return ( (ByteArrayOutputStream) out ).toByteArray();
  }

  public DataInputStream toDataInputStream() {
    return new DataInputStream( new ByteArrayInputStream( getBuffer() ) );
  }
}
