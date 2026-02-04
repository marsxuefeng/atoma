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
import java.util.concurrent.TimeUnit;

/** Test case for BARRIER-TC-046: Single participant barrier behavior verification. */
public class BarrierTc046Test extends BaseTest {

  @DisplayName("BARRIER-TC-046: 单个参与者的屏障行为验证")
  @Test
  public void testSingleParticipantBarrier() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);
    Lease lease = client.grantLease();
    final String barrierId = "BARRIER-TC-046";
    final int parties = 1;
    final CyclicBarrier barrier = lease.getCyclicBarrier(barrierId, parties);

    try {
      // First await should pass immediately
      Assertions.assertDoesNotThrow(
          () -> barrier.await(1, TimeUnit.SECONDS), "Await with 1 party should pass immediately");
      Assertions.assertEquals(
          0, barrier.getNumberWaiting(), "Number waiting should be 0 after immediate pass");

      // Second await should also pass immediately, proving reusability
      Assertions.assertDoesNotThrow(
          () -> barrier.await(1, TimeUnit.SECONDS),
          "Second await with 1 party should also pass immediately");
      Assertions.assertEquals(
          0, barrier.getNumberWaiting(), "Number waiting should be 0 after second immediate pass");

    } finally {
      barrier.close();
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
