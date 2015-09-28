package org.pentaho.di.trans.dataservice.jdbc;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by bmorrise on 9/28/15.
 */
public class ThinConnectionTest {

  @Test
  public void testThinConnection() throws Exception {
    String host = "localhost";
    String port = "9080";
    String webAppName = "pentaho-di";
    String proxyHostName = "proxyhostname";
    String proxyPort = "9081";
    String nonProxyHosts = "nonproxyhost";
    String debugTrans = "debugTrans";
    String debugLog = "true";
    String secure = "true";
    String local = "false";

    String
        url =
        "jdbc:pdi://" + host + ":" + port + "/kettle?webappname=" + webAppName + "&proxyhostname=" + proxyHostName
            + "&proxyport=" + proxyPort + "&nonproxyhosts=" + nonProxyHosts + "&debugtrans=" + debugTrans + "&debuglog="
            + debugLog + "&secure=" + secure + "&local=" + local;
    String username = "username";
    String password = "password";

    ThinConnection thinConnection = new ThinConnection( url, username, password );

    assertEquals( host, thinConnection.getHostname() );
    assertEquals( port, thinConnection.getPort() );
    assertEquals( webAppName, thinConnection.getWebAppName() );
    assertEquals( proxyHostName, thinConnection.getProxyHostname() );
    assertEquals( proxyPort, thinConnection.getProxyPort() );
    assertEquals( nonProxyHosts, thinConnection.getNonProxyHosts() );
    assertEquals( debugTrans, thinConnection.getDebugTransFilename() );
    assertEquals( true, thinConnection.isDebuggingRemoteLog() );
    assertEquals( true, thinConnection.isSecure() );
    assertEquals( false, thinConnection.isLocal() );
  }

}
