package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.FairIOController;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

// TODO TODO afegir comentaris a tot arreu!!!
// TODO TODO a mes de la descripcio de les classes propies

/**
 * Disk management using cgroups. Receive messages from NameNode via TCP socket
 * Assignment of weights by cgroups directories.
 */
public class FairIODataNodeDiskController implements Runnable {
  public static final Log LOG = LogFactory.getLog(FairIODataNodeDiskController.class);
  private ControlGroup cGroup;

  private ServerSocket serverSocket;

  public FairIODataNodeDiskController() {
    cGroup = new ControlGroup.BlkIOControlGroup();

    try {
      serverSocket = new ServerSocket(DFSConfigKeys.DFS_DATANODE_FAIR_IO_DISK_PORT);
    } catch (IOException e) {
      LOG.error(e.getMessage());
    }
  }

  private void setCgroupWeights(long classId, long weight) {
    LOG.info("CAMAMILLA FairIODataNodeDiskController.setCgroupWeights "+classId+"=>"+weight);     // TODO TODO
    // Set new value to specific directory
    String path = cGroup.createSubDirectory(String.valueOf(classId));
    ControlGroup group = new ControlGroup.BlkIOControlGroup(path);

    // Move process to corresponding CGroup
    long tid = (long) ControlGroup.LinuxHelper.gettid();
    group.addTaskToGroup(String.valueOf(tid));

    group.setLongParameter(ControlGroup.BlkIOControlGroup.IO_WEIGHT, weight);
  }

  @Override
  public void run() {
    boolean shouldRun = true;

    while (shouldRun) {
      try {
        // Wait until new request
        Socket connectionSocket = serverSocket.accept();
        BufferedReader inFromNN =
          new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));

        // fullLine format or "classId:weight;classId:weight;classId:weight;" or FairIOController.EXIT_SIGNAL
        String fullLine = inFromNN.readLine();
        LOG.info("CAMAMILLA FairIODataNodeDiskController received message="+fullLine);     // TODO TODO
        if (fullLine.equals(FairIOController.EXIT_SIGNAL)) {
          shouldRun = false;
        } else {
          for (String line : fullLine.split(";")){
            String stClassId = line.substring(0, line.indexOf(":"));
            String stWeight = line.substring(line.indexOf(":") + 1);

            long classId = Long.parseLong(stClassId);
            long weight = Long.parseLong(stWeight);

            setCgroupWeights(classId, weight);
          }
        }
        inFromNN.close();
      } catch (IOException e) {
        LOG.error(e.getMessage());
      }
    }
    LOG.info("CAMAMILLA FairIODataNodeDiskController exit");     // TODO TODO
  }
}
