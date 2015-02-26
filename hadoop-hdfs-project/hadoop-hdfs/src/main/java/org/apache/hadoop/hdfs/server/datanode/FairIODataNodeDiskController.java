package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

  public FairIODataNodeDiskController(int port) {
    LOG.info("CAMAMILLA FairIODataNodeDiskController constructor");     // TODO TODO log
    cGroup = new ControlGroup.BlkIOControlGroup();

    try {
      serverSocket = new ServerSocket(port);
    } catch (IOException e) {
      LOG.error("CAMAMILLA ERROR: "+e.getMessage());
    }

    LOG.info("CAMAMILLA FairIODataNodeDiskController end constructor");     // TODO TODO log
  }

  private void setCgroupWeights(long classId, long weight) {
    // Set new value to specific directory
    String path = cGroup.createSubDirectory(String.valueOf(classId));
    ControlGroup group = new ControlGroup.BlkIOControlGroup(path);

    // Move process to corresponding CGroup
    long tid = (long) ControlGroup.LinuxHelper.gettid();
    group.addTaskToGroup(String.valueOf(tid));
    group.setLongParameter(ControlGroup.BlkIOControlGroup.IO_WEIGHT, weight);
    LOG.info("CAMAMILLA FairIODataNodeDiskController.setCgroupWeights "+classId+"=>"+weight+" amb tid="+tid);     // TODO TODO log
  }

  @Override
  public void run() {
    LOG.info("CAMAMILLA FairIODataNodeDiskController.run");     // TODO TODO log
    boolean shouldRun = true;

    while (shouldRun) {
      try {
        LOG.info("CAMAMILLA FairIODataNodeDiskController Wait until new request on "+serverSocket.getInetAddress().getHostAddress());     // TODO TODO log
        // Wait until new request
        Socket connectionSocket = serverSocket.accept();
        BufferedReader inFromNN =
          new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));

        // fullLine format => "classId:weight;classId:weight;classId:weight;" or FairIOController.EXIT_SIGNAL
        String fullLine = inFromNN.readLine();
        LOG.info("CAMAMILLA FairIODataNodeDiskController received message="+fullLine);     // TODO TODO log
        if (fullLine.equals(FairIOController.EXIT_SIGNAL)) {
          shouldRun = false;
          LOG.info("CAMAMILLA FairIODataNodeDiskController now will exit");     // TODO TODO log
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
    LOG.info("CAMAMILLA FairIODataNodeDiskController exit");     // TODO TODO log
  }
}
