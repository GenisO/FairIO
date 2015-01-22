package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * FairIOController Tester.
 *
 * @author AST
 * @since <pre>
 * ene 16, 2015
 * </pre>
 * @version 1.0
 */
public class TestFairIOController {

   FairIOController controller;
   String ipDN1;
   String ipDN2;
   String ipDN3;

   long w1;
   long w2;
   long w3;
   long w4;
   long w5;

   long c1;
   long c2;
   long c3;
   long c4;
   long c5;

   DatanodeID dn1;
   DatanodeID dn2;
   DatanodeID dn3;

   @Before
   public void before() throws Exception {
      ipDN1 = "1";
      ipDN2 = "2";
      ipDN3 = "3";

      w1 = 100;
      w2 = 200;
      w3 = 300;
      w4 = 400;
      w5 = 500;

      c1 = 16831;
      c2 = 16832;
      c3 = 16833;
      c4 = 16834;
      c5 = 16835;

      dn1 = new DatanodeID(ipDN1, "d1", "d1", 1234, 1235, 1236, 1237);
      dn2 = new DatanodeID(ipDN2, "d2", "d2", 1234, 1235, 1236, 1237);
      dn3 = new DatanodeID(ipDN3, "d3", "d3", 1234, 1235, 1236, 1237);

      controller = new FairIOController(0);
      controller.setTest(true);
   }

   @After
   public void after() throws Exception {
      //System.out.println(controller);
   }

   /**
    *
    * Method: registerDatanode(DatanodeID datanodeID)
    *
    */
   @Test
   public void testRegisterDatanode() throws Exception {
      controller.registerDatanode(dn1);
      assert controller.existsDatanode(dn1);
   }

   /**
    *
    * Method: existsClassInfo(long classId)
    *
    */
   @Test
   public void testExistsClassInfo() throws Exception {
      controller.setClassWeight(c1);
      assert controller.existsClassInfo(c1);
      assert !controller.existsClassInfo(c2);
   }

   /**
    *
    * Method: setClassWeight(long classId)
    *
    */
   @Test
   public void testSetClassWeightClassId() throws Exception {
      controller.setClassWeight(c2);
      assert controller.existsClassInfo(c2);
   }

   /**
    *
    * Method: setClassWeight(long classId, long weight)
    *
    */
   @Test
   public void testSetClassWeightForClassIdWeight() throws Exception {
      controller.setClassWeight(c1, w1);
      assert controller.existsClassInfo(c1);
   }

   /**
    *
    * Method: addDatanodeToClass(long classId, String datanodeUUID)
    *
    */
   @Test
   public void testAddDatanodeToClass() throws Exception {
      controller.setClassWeight(c1, w1);
      controller.registerDatanode(dn1);
      controller.addDatanodeToClass(c1, dn1.getDatanodeUuid());

      assert controller.getClassWeight(c1,dn1.getDatanodeUuid())!=0;
   }

   /**
    *
    * Method: removeDatanodeFromClass(long classId, String datanodeUUID)
    *
    */
   @Test
   public void testRemoveDatanodeFromClass() throws Exception {
      controller.setClassWeight(c1, w1);
      controller.registerDatanode(dn1);
      controller.addDatanodeToClass(c1, dn1.getDatanodeUuid());
      controller.removeDatanodeFromClass(c1, dn1.getDatanodeUuid());

      assert controller.getClassWeight(c1,dn1.getDatanodeUuid())==0;
   }

   /**
    *
    * Method: getClassWeight(long classId)
    *
    */
   @Test
   public void testGetClassWeightClassId() throws Exception {
      controller.setClassWeight(c1, w1);
      assert controller.getClassWeight(c1) == w1;
   }

   /**
    *
    * Method: getClassWeight(long classId, String dnuuid)
    *
    */
   @Test
   public void testGetClassWeightForClassIdDnuuid() throws Exception {
      controller.setClassWeight(c1, w1);
      controller.registerDatanode(dn1);
      controller.addDatanodeToClass(c1, dn1.getDatanodeUuid());
      System.out.println("CAMAMILLA\n" + controller);
      long recieved = controller.getClassWeight(c1, dn1.getDatanodeUuid());
      // double check for lost decimals
      assert (recieved == w1 || w1-recieved<=1);
   }

   /**
    *
    * Method: computeShares()
    *
    */
   @Test
   public void testComputeShares() throws Exception {
      // TODO: Test goes here...
   }

   /**
    *
    * Method: initializeAllShares()
    *
    */
   @Test
   @Deprecated
   public void testInitializeAllShares() throws Exception {
   }

   /**
    *
    * Method: initializeShares(FairIOClassInfo classInfo)
    *
    */
   @Test
   @Deprecated
   public void testInitializeShares() throws Exception {
   }

   /**
    *
    * Method: shutdown()
    *
    */
   @Test
   public void testShutdown() throws Exception {
      // TODO: Test goes here...
   }

   /**
    *
    * Method: weightsReady()
    *
    */
   @Test
   public void testWeightsReady() throws Exception {
      // TODO: Test goes here...
   }

   /**
    *
    * Method: sendMessage(String ip, String sentence)
    *
    */
   @Test
   public void testSendMessage() throws Exception {
      // TODO: Test goes here...
   }

   /**
    *
    * Method: getUtility(FairIOClassInfo classInfo)
    *
    */
   @Test
   public void testGetUtility() throws Exception {
      // TODO: Test goes here...
   }

   /**
    *
    * Method: getUtilities()
    *
    */
   @Test
   public void testGetUtilities() throws Exception {
      // TODO: Test goes here...
   }

   /**
    *
    * Method: isUtilityConverged(Map<FairIOClassInfo, BigDecimal>
    * previousUtilities)
    *
    */
   @Test
   public void testIsUtilityConverged() throws Exception {
      // TODO: Test goes here...
   }

   /**
    *
    * Method: computeSharesByClass(FairIOClassInfo classInfo)
    *
    */
   @Test
   public void testComputeSharesByClass() throws Exception {
      // TODO: Test goes here...
   }

   /**
    *
    * Method: sqrt(BigDecimal A)
    *
    */
   @Test
   public void testSqrt() throws Exception {
      // TODO: Test goes here...
   }

}
