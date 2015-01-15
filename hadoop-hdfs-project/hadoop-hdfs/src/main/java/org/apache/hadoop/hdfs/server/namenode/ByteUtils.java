package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Auxiliary class to manage conversion between byte[] to float and opposite case
 */
public class ByteUtils {
  public static final Log LOG = LogFactory.getLog(FSNamesystem.class);

  public static byte[] floatToByte(Float f) {
    return String.valueOf(f).getBytes();
  }

  public static float bytesToFloat(byte[] b) {
    return new Float(new String(b));
  }

  public static byte[] longToByte(Long l) {
    return String.valueOf(l).getBytes();
  }

  public static long bytesToLong(byte[] b) {
    return new Long(new String(b));
  }
}
