package atoma.test.barrier;

import atoma.api.Lease;
import atoma.api.synchronizer.CyclicBarrier;
import atoma.core.AtomaClient;
import atoma.storage.mongo.MongoCoordinationStore;
import atoma.test.BaseTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Test case for BARRIER-TC-006: Initialize the same barrier ID with different numbers of parties on
 * different nodes.
 */
public class BarrierTc006Test extends BaseTest {

  @DisplayName("BARRIER-TC-006: 在不同节点使用不同参与者数量初始化相同屏障ID")
  @Test
  public void testInitializeWithDifferentParties() throws Exception {
    final String barrierId = "BARRIER-TC-006";
    final int parties1 = 2;
    final int parties2 = 3;

    // Client 1 setup
    MongoCoordinationStore store1 = newMongoCoordinationStore();
    ScheduledExecutorService executor1 = newScheduledExecutorService();
    AtomaClient client1 = new AtomaClient(executor1, store1);
    Lease lease1 = client1.grantLease();

    // Client 2 setup
    MongoCoordinationStore store2 = newMongoCoordinationStore();
    ScheduledExecutorService executor2 = newScheduledExecutorService();
    AtomaClient client2 = new AtomaClient(executor2, store2);
    Lease lease2 = client1.grantLease();

    CyclicBarrier barrier1 = null;
    try {
      // Client 1 initializes the barrier successfully
      barrier1 = lease1.getCyclicBarrier(barrierId, parties1);
      Assertions.assertNotNull(barrier1);
      Assertions.assertEquals(parties1, barrier1.getParties());

      // Client 2 attempts to initialize the same barrier with different parties
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> lease2.getCyclicBarrier(barrierId, parties2),
          "Should throw IllegalArgumentException when parties mismatch");

    } finally {
      if (barrier1 != null) {
        barrier1.close();
      }
      client1.close();
      executor1.shutdownNow();
      store1.close();

      client2.close();
      executor2.shutdownNow();
      store2.close();
    }
  }
}
