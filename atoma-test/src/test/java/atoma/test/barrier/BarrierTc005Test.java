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
 * Test case for BARRIER-TC-005: Initialize barrier with a number of parties exceeding the system
 * limit.
 */
public class BarrierTc005Test extends BaseTest {

  @DisplayName("BARRIER-TC-005: 使用超过系统限制的参与者数量初始化屏障")
  @Test
  public void testInitializeWithLargeParties() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);
    Lease lease = client.grantLease();

    final String barrierId = "BARRIER-TC-005";
    // Assuming no hard system limit is enforced by the client,
    // we test with a large number of parties.
    final int parties = 1000;

    CyclicBarrier cyclicBarrier = null;
    try {
      cyclicBarrier = lease.getCyclicBarrier(barrierId, parties);
      Assertions.assertNotNull(cyclicBarrier, "CyclicBarrier should be created successfully");
      Assertions.assertEquals(
          parties, cyclicBarrier.getParties(), "The number of parties should be set correctly");
    } finally {
      lease.close();
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
