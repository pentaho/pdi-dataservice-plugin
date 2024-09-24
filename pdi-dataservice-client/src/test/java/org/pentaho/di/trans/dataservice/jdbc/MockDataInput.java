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
