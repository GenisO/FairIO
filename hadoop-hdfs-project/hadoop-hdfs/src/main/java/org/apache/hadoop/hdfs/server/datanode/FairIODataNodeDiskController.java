package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.net.TcpPeerServer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by DEIM on 12/01/15.
 */
public class FairIODataNodeDiskController implements Runnable {
  public static final Log LOG = LogFactory.getLog(FairIODataNodeDiskController.class);
  private ControlGroup cGroup;

  private TcpPeerServer tcpPeerServer;
  ServerSocket serverSocket;


  public FairIODataNodeDiskController() {
    this.cGroup = new ControlGroup.BlkIOControlGroup();

    try {
      serverSocket = new ServerSocket(DFSConfigKeys.DFS_DATANODE_FAIRIODISK_PORT);
    } catch (IOException e) {
      LOG.error(e.getMessage());
    }

  }

  private void setCgroupWeights(long classId, long weight) {
    LOG.info("CAMAMILLA FairIODataNodeDiskController.setCgroupWeights "+classId+"=>"+weight);     // TODO TODO
    // Move process to corresponding CGroup
    String path = cGroup.createSubDirectory(String.valueOf(classId));
    ControlGroup group = new ControlGroup.BlkIOControlGroup(path);
    long tid = (long) ControlGroup.LinuxHelper.gettid();
    group.addTaskToGroup(String.valueOf(tid));
    group.setLongParameter(ControlGroup.BlkIOControlGroup.IO_WEIGHT, weight);
  }

  @Override
  public void run() {
    boolean shouldRun = true;

    while (shouldRun) {
      try {
        Socket connectionSocket = serverSocket.accept();
        BufferedReader inFromNN =
          new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
        String line = inFromNN.readLine();
        LOG.info("CAMAMILLA FairIODataNodeDiskController received message="+line);     // TODO TODO
        if (line.equals("EXIT")) {
          shouldRun = false;
        } else {
          String stClassId = line.substring(0, line.indexOf(":"));
          String stWeight = line.substring(line.indexOf(":") + 1);

          long classId = Long.parseLong(stClassId);
          long weight = Long.parseLong(stWeight);

          setCgroupWeights(classId, weight);
        }
        inFromNN.close();
      } catch (IOException e) {
        LOG.error(e.getMessage());
      }
    }
    LOG.info("CAMAMILLA FairIODataNodeDiskController exit");     // TODO TODO
  }
}
