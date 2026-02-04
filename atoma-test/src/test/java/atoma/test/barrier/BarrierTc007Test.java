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

/** Test case for BARRIER-TC-007: Repeatedly initialize the same barrier ID (idempotency test). */
public class BarrierTc007Test extends BaseTest {

  @DisplayName("BARRIER-TC-007: 重复初始化相同屏障ID（幂等性测试）")
  @Test
  public void testIdempotentInitialization() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);
    Lease lease = client.grantLease();

    final String barrierId = "BARRIER-TC-007";
    final int parties = 2;

    CyclicBarrier barrier1 = null;
    CyclicBarrier barrier2 = null;
    try {
      // First initialization
      barrier1 = lease.getCyclicBarrier(barrierId, parties);
      Assertions.assertNotNull(barrier1, "First barrier should be initialized");
      Assertions.assertEquals(parties, barrier1.getParties());

      // Second initialization with the same parameters
      barrier2 = lease.getCyclicBarrier(barrierId, parties);
      Assertions.assertNotNull(barrier2, "Second barrier should also be initialized");
      Assertions.assertEquals(parties, barrier2.getParties());

      // The instances might be different, but they refer to the same distributed object
      Assertions.assertEquals(barrier1.getResourceId(), barrier2.getResourceId());
      Assertions.assertEquals(barrier1.getParties(), barrier2.getParties());

    } finally {
      if (barrier1 != null) {
        barrier1.close();
      }
      if (barrier2 != null) {
        barrier2.close();
      }
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
