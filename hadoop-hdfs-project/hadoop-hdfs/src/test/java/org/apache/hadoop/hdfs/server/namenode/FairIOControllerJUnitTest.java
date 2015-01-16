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
public class FairIOControllerJUnitTest {

   FairIOController controller;
   String ipDN1 = "1";
   String ipDN2 = "2";
   String ipDN3 = "3";

   long w1 = 100;
   long w2 = 200;
   long w3 = 300;
   long w4 = 400;
   long w5 = 500;

   long c1 = 16831;
   long c2 = 16832;
   long c3 = 16833;
   long c4 = 16834;
   long c5 = 16835;

   DatanodeID dn1 = new DatanodeID(ipDN1, "d1", "d1", 1234, 1235, 1236, 1237);
   DatanodeID dn2 = new DatanodeID(ipDN2, "d2", "d2", 1234, 1235, 1236, 1237);
   DatanodeID dn3 = new DatanodeID(ipDN3, "d3", "d3", 1234, 1235, 1236, 1237);

   @Before
   public void before() throws Exception {
      controller = new FairIOController();
   }

   @After
   public void after() throws Exception {
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
   }

   /**
    *
    * Method: setClassWeight(long classId)
    *
    */
   @Test
   public void testSetClassWeightClassId() throws Exception {
      // TODO: Test goes here...
   }

   /**
    *
    * Method: setClassWeight(long classId, long weight)
    *
    */
   @Test
   public void testSetClassWeightForClassIdWeight() throws Exception {
      // TODO: Test goes here...
   }

   /**
    *
    * Method: addDatanodeToClass(long classId, String datanodeUUID)
    *
    */
   @Test
   public void testAddDatanodeToClass() throws Exception {
      // TODO: Test goes here...
   }

   /**
    *
    * Method: removeDatanodeFromClass(long classId, String datanodeUUID)
    *
    */
   @Test
   public void testRemoveDatanodeFromClass() throws Exception {
      // TODO: Test goes here...
   }

   /**
    *
    * Method: getClassWeight(long classId)
    *
    */
   @Test
   public void testGetClassWeightClassId() throws Exception {
      // TODO: Test goes here...
   }

   /**
    *
    * Method: getClassWeight(long classId, String dnuuid)
    *
    */
   @Test
   public void testGetClassWeightForClassIdDnuuid() throws Exception {
      // TODO: Test goes here...
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
   public void testInitializeAllShares() throws Exception {
      // TODO: Test goes here...
   }

   /**
    *
    * Method: initializeShares(FairIOClassInfo classInfo)
    *
    */
   @Test
   public void testInitializeShares() throws Exception {
      // TODO: Test goes here...
   }

   /**
    *
    * Method: toString()
    *
    */
   @Test
   public void testToString() throws Exception {
      // TODO: Test goes here...
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
		/*
		 * try { Method method =
		 * FairIOController.getClass().getMethod("weightsReady");
		 * method.setAccessible(true); method.invoke(<Object>, <Parameters>); }
		 * catch(NoSuchMethodException e) { } catch(IllegalAccessException e) {
		 * } catch(InvocationTargetException e) { }
		 */
   }

   /**
    *
    * Method: sendMessage(String ip, String sentence)
    *
    */
   @Test
   public void testSendMessage() throws Exception {
      // TODO: Test goes here...
		/*
		 * try { Method method =
		 * FairIOController.getClass().getMethod("sendMessage",
		 * String.class, String.class); method.setAccessible(true);
		 * method.invoke(<Object>, <Parameters>); } catch(NoSuchMethodException
		 * e) { } catch(IllegalAccessException e) { }
		 * catch(InvocationTargetException e) { }
		 */
   }

   /**
    *
    * Method: getUtility(FairIOClassInfo classInfo)
    *
    */
   @Test
   public void testGetUtility() throws Exception {
      // TODO: Test goes here...
		/*
		 * try { Method method =
		 * FairIOController.getClass().getMethod("getUtility",
		 * FairIOClassInfo.class); method.setAccessible(true);
		 * method.invoke(<Object>, <Parameters>); } catch(NoSuchMethodException
		 * e) { } catch(IllegalAccessException e) { }
		 * catch(InvocationTargetException e) { }
		 */
   }

   /**
    *
    * Method: getUtilities()
    *
    */
   @Test
   public void testGetUtilities() throws Exception {
      // TODO: Test goes here...
		/*
		 * try { Method method =
		 * FairIOController.getClass().getMethod("getUtilities");
		 * method.setAccessible(true); method.invoke(<Object>, <Parameters>); }
		 * catch(NoSuchMethodException e) { } catch(IllegalAccessException e) {
		 * } catch(InvocationTargetException e) { }
		 */
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
		/*
		 * try { Method method =
		 * FairIOController.getClass().getMethod("isUtilityConverged",
		 * Map<FairIOClassInfo,.class); method.setAccessible(true);
		 * method.invoke(<Object>, <Parameters>); } catch(NoSuchMethodException
		 * e) { } catch(IllegalAccessException e) { }
		 * catch(InvocationTargetException e) { }
		 */
   }

   /**
    *
    * Method: computeSharesByClass(FairIOClassInfo classInfo)
    *
    */
   @Test
   public void testComputeSharesByClass() throws Exception {
      // TODO: Test goes here...
		/*
		 * try { Method method =
		 * FairIOController.getClass().getMethod("computeSharesByClass",
		 * FairIOClassInfo.class); method.setAccessible(true);
		 * method.invoke(<Object>, <Parameters>); } catch(NoSuchMethodException
		 * e) { } catch(IllegalAccessException e) { }
		 * catch(InvocationTargetException e) { }
		 */
   }

   /**
    *
    * Method: sqrt(BigDecimal A)
    *
    */
   @Test
   public void testSqrt() throws Exception {
      // TODO: Test goes here...
		/*
		 * try { Method method =
		 * FairIOController.getClass().getMethod("sqrt", BigDecimal.class);
		 * method.setAccessible(true); method.invoke(<Object>, <Parameters>); }
		 * catch(NoSuchMethodException e) { } catch(IllegalAccessException e) {
		 * } catch(InvocationTargetException e) { }
		 */
   }

}
