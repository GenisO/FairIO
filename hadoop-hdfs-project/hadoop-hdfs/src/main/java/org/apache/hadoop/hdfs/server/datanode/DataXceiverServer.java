/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DWRRDFSClient;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.net.PeerServer;
import org.apache.hadoop.hdfs.protocol.datatransfer.DWRRManager;
import org.apache.hadoop.hdfs.server.namenode.ByteUtils;
import org.apache.hadoop.hdfs.server.namenode.FairIOController;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Daemon;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.channels.AsynchronousCloseException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Server used for receiving/sending a block of data.
 * This is created to listen for requests from clients or
 * other DataNodes.  This small server does not use the
 * Hadoop IPC mechanism.
 */
public class DataXceiverServer implements Runnable {
  public static final Log LOG = DataNode.LOG;
  
  private final PeerServer peerServer;
  private final DataNode datanode;
  private final HashMap<Peer, Thread> peers = new HashMap<Peer, Thread>();
  private final boolean concurrentDWRR;
  private DWRRManager dwrrmanager;
  private final boolean shedulerDWRR;
  private DWRRDFSClient dfs;
  private boolean closed = false;
  private Map<Long, Float> allRequestMap;
  private boolean isCgroupManaged;
  
  /**
   * Maximal number of concurrent xceivers per node.
   * Enforcing the limit is required in order to avoid data-node
   * running out of memory.
   */
  int maxXceiverCount =
    DFSConfigKeys.DFS_DATANODE_MAX_RECEIVER_THREADS_DEFAULT;

  /** A manager to make sure that cluster balancing does not
   * take too much resources.
   * 
   * It limits the number of block moves for balancing and
   * the total amount of bandwidth they can use.
   */
  static class BlockBalanceThrottler extends DataTransferThrottler {
   private int numThreads;
   private int maxThreads;
   
   /**Constructor
    * 
    * @param bandwidth Total amount of bandwidth can be used for balancing 
    */
   private BlockBalanceThrottler(long bandwidth, int maxThreads) {
     super(bandwidth);
     this.maxThreads = maxThreads;
     LOG.info("Balancing bandwith is "+ bandwidth + " bytes/s");
     LOG.info("Number threads for balancing is "+ maxThreads);
   }
   
   /** Check if the block move can start. 
    * 
    * Return true if the thread quota is not exceeded and 
    * the counter is incremented; False otherwise.
    */
   synchronized boolean acquire() {
     if (numThreads >= maxThreads) {
       return false;
     }
     numThreads++;
     return true;
   }
   
   /** Mark that the move is completed. The thread counter is decremented. */
   synchronized void release() {
     numThreads--;
   }
  }

  final BlockBalanceThrottler balanceThrottler;
  
  /**
   * We need an estimate for block size to check if the disk partition has
   * enough space. For now we set it to be the default block size set
   * in the server side configuration, which is not ideal because the
   * default block size should be a client-size configuration. 
   * A better solution is to include in the header the estimated block size,
   * i.e. either the actual block size or the default block size.
   */
  final long estimateBlockSize;
  
  
  DataXceiverServer(PeerServer peerServer, Configuration conf,
      DataNode datanode) {
    
    this.peerServer = peerServer;
    this.datanode = datanode;

    this.maxXceiverCount = 
      conf.getInt(DFSConfigKeys.DFS_DATANODE_MAX_RECEIVER_THREADS_KEY,
                  DFSConfigKeys.DFS_DATANODE_MAX_RECEIVER_THREADS_DEFAULT);
    
    this.estimateBlockSize = conf.getLongBytes(DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
        DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);
    
    //set up parameter for cluster balancing
    this.balanceThrottler = new BlockBalanceThrottler(
        conf.getLong(DFSConfigKeys.DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_KEY,
            DFSConfigKeys.DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_DEFAULT),
        conf.getInt(DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY,
            DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_DEFAULT));
		this.shedulerDWRR = conf.getBoolean(DFSConfigKeys.DFS_DATANODE_XCEIVER_DWRR_MODE_KEY, DFSConfigKeys.DFS_DATANODE_XCEIVER_DWRR_DEFAULT);
    this.concurrentDWRR = conf.getBoolean(DFSConfigKeys.DFS_DATANODE_XCEIVER_DWRR_MODE_CONCURRENT_KEY, DFSConfigKeys.DFS_DATANODE_XCEIVER_DWRR_DEFAULT);

    LOG.info("CAMAMILLA DWRR?="+shedulerDWRR+" new concurrent version?="+concurrentDWRR);
    //cache of the class weights
    this.allRequestMap = new ConcurrentHashMap<Long, Float>();
    this.isCgroupManaged = conf.getBoolean(DFSConfigKeys.DFS_DATANODE_XCEIVER_DWRR_CGROUPS_MODE_KEY, DFSConfigKeys.DFS_DATANODE_XCEIVER_DWRR_CGROUPS_DEFAULT);

    try {
      this.dfs = new DWRRDFSClient(NameNode.getAddress(conf), conf);
      if (concurrentDWRR) {
        this.dwrrmanager = new DWRRManager(conf, dfs, datanode);
      } else {
        this.dwrrmanager = new DWRRManager(conf, dfs, datanode);
      }
    } catch (IOException e) {
      LOG.error("CAMAMILLA DataXceiverServer error dfs "+e.getMessage());      // TODO TODO log
    }
  }

  @Override
  public void run() {
    Peer peer = null;
    while (datanode.shouldRun && !datanode.shutdownForUpgrade) {
      try {
        peer = peerServer.accept();

        // Make sure the xceiver count is not exceeded
        int curXceiverCount = datanode.getXceiverCount();
        if (curXceiverCount > maxXceiverCount) {
          throw new IOException("Xceiver count " + curXceiverCount
              + " exceeds the limit of concurrent xcievers: "
              + maxXceiverCount);
        }

        if (shedulerDWRR) {     // TODO TODO llançadora de DWRR
					new Daemon(datanode.threadGroup,
						DWRRDataXceiver.create(peer, datanode, this, dwrrmanager))
						.start();
				} else {
					new Daemon(datanode.threadGroup,
						DataXceiver.create(peer, datanode, this))
						.start();
				}
      } catch (SocketTimeoutException ignored) {
        // wake up to see if should continue to run
      } catch (AsynchronousCloseException ace) {
        // another thread closed our listener socket - that's expected during shutdown,
        // but not in other circumstances
        if (datanode.shouldRun && !datanode.shutdownForUpgrade) {
          LOG.warn(datanode.getDisplayName() + ":DataXceiverServer: ", ace);
        }
      } catch (IOException ie) {
        IOUtils.cleanup(null, peer);
        LOG.warn(datanode.getDisplayName() + ":DataXceiverServer: ", ie);
      } catch (OutOfMemoryError ie) {
        IOUtils.cleanup(null, peer);
        // DataNode can run out of memory if there is too many transfers.
        // Log the event, Sleep for 30 seconds, other transfers may complete by
        // then.
        LOG.warn("DataNode is out of memory. Will retry in 30 seconds.", ie);
        try {
          Thread.sleep(30 * 1000);
        } catch (InterruptedException e) {
          // ignore
        }
      } catch (Throwable te) {
        LOG.error(datanode.getDisplayName()
            + ":DataXceiverServer: Exiting due to: ", te);
        datanode.shouldRun = false;
      }
    }

    // Close the server to stop reception of more requests.
    try {
      peerServer.close();
      closed = true;
    } catch (IOException ie) {
      LOG.warn(datanode.getDisplayName()
          + " :DataXceiverServer: close exception", ie);
    }

    // if in restart prep stage, notify peers before closing them.
    if (datanode.shutdownForUpgrade) {
      restartNotifyPeers();
      // Each thread needs some time to process it. If a thread needs
      // to send an OOB message to the client, but blocked on network for
      // long time, we need to force its termination.
      LOG.info("Shutting down DataXceiverServer before restart");
      // Allow roughly up to 2 seconds.
      for (int i = 0; getNumPeers() > 0 && i < 10; i++) {
        try {
          Thread.sleep(200);
        } catch (InterruptedException e) {
          // ignore
        }
      }
    }
    // Close all peers.
    closeAllPeers();
  }

  void kill() {
    assert (datanode.shouldRun == false || datanode.shutdownForUpgrade) :
      "shoudRun should be set to false or restarting should be true"
      + " before killing";
    try {
      this.peerServer.close();
      this.closed = true;
    } catch (IOException ie) {
      LOG.warn(datanode.getDisplayName() + ":DataXceiverServer.kill(): ", ie);
    }
  }
  
  synchronized void addPeer(Peer peer, Thread t) throws IOException {
    if (closed) {
      throw new IOException("Server closed.");
    }
    peers.put(peer, t);
  }

  synchronized void closePeer(Peer peer) {
    peers.remove(peer);
    IOUtils.cleanup(null, peer);
  }

  // Notify all peers of the shutdown and restart.
  // datanode.shouldRun should still be true and datanode.restarting should
  // be set true before calling this method.
  synchronized void restartNotifyPeers() {
    assert (datanode.shouldRun == true && datanode.shutdownForUpgrade);
    for (Peer p : peers.keySet()) {
      // interrupt each and every DataXceiver thread.
      peers.get(p).interrupt();
    }
  }

  // Close all peers and clear the map.
  synchronized void closeAllPeers() {
    LOG.info("Closing all peers.");
    for (Peer p : peers.keySet()) {
      IOUtils.cleanup(LOG, p);
    }
    peers.clear();
  }

  synchronized boolean isCgroupManaged() {
    return this.isCgroupManaged;
  }

  synchronized float getClassWeight(long classId) {
    if (!this.allRequestMap.containsKey(classId)) {
      float weight;
      Map<String, byte[]> xattr = null;
      try {
        xattr = dfs.getXAttrs(classId, datanode.getDatanodeId().getDatanodeUuid());

        if (xattr == null) {
          LOG.error("CAMAMILLA DataXceiverServer.opReadBlock.list no te atribut weight");      // TODO TODO log
          weight = FairIOController.DEFAULT_WEIGHT;
        } else {
          LOG.info("CAMAMILLA DataXceiverServer.opReadBlock.list fer el get de user." + DWRRManager.nameWeight);      // TODO TODO log
          weight = ByteUtils.bytesToFloat(xattr.get("user." + DWRRManager.nameWeight));
        }
      } catch (IOException e) {
        LOG.error("CAMAMILLA DataXceiverServer.opReadBlock.list ERROR al getXattr " + e.getMessage());      // TODO TODO log
        weight = FairIOController.DEFAULT_WEIGHT;
      }
      allRequestMap.put(classId, weight);
    }
    float _weight = this.allRequestMap.get(classId);
    float sum_weights = 0.0F;
    for (float weight : this.allRequestMap.values()) {
      sum_weights += weight;
    }
    _weight = (_weight/sum_weights) * 1000.0F;
    return _weight;
  }

//  synchronized DWRRDFSClient getDFSClient() { return this.dfs; }

  // Return the number of peers.
  synchronized int getNumPeers() {
    return peers.size();
  }

  synchronized void releasePeer(Peer peer) {
    peers.remove(peer);
  }
}