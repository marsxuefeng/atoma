package atoma.test.barrier;

import atoma.api.Lease;
import atoma.api.synchronizer.CyclicBarrier;
import atoma.core.AtomaClient;
import atoma.storage.mongo.MongoCoordinationStore;
import atoma.test.BaseTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Test case for BARRIER-TC-021: Barrier resets automatically after all nodes pass. */
public class BarrierTc021Test extends BaseTest {

  @DisplayName("BARRIER-TC-021: 所有节点通过后自动重置屏障")
  @Test
  public void testAutomaticResetAfterPass() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);
    Lease lease = client.grantLease();
    final String barrierId = "BARRIER-TC-021";
    final int parties = 2;
    final CyclicBarrier barrier = lease.getCyclicBarrier(barrierId, parties);

    try {
      // First pass
      final CountDownLatch firstPassLatch = new CountDownLatch(parties);
      for (int i = 0; i < parties; i++) {
        new Thread(
                () -> {
                  try {
                    barrier.await(5, TimeUnit.SECONDS);
                    firstPassLatch.countDown();
                  } catch (Exception e) {
                    Assertions.fail("Await should not fail on first pass", e);
                  }
                })
            .start();
      }
      Assertions.assertTrue(
          firstPassLatch.await(6, TimeUnit.SECONDS), "All parties should pass the first barrier");
      TimeUnit.MILLISECONDS.sleep(200);
      Assertions.assertEquals(
          0, barrier.getNumberWaiting(), "Waiting count should be 0 after first pass");

      // Second pass (verifies automatic reset)
      final CountDownLatch secondPassLatch = new CountDownLatch(parties);
      for (int i = 0; i < parties; i++) {
        new Thread(
                () -> {
                  try {
                    barrier.await(5, TimeUnit.SECONDS);
                    secondPassLatch.countDown();
                  } catch (Exception e) {
                    Assertions.fail("Await should not fail on second pass", e);
                  }
                })
            .start();
      }
      Assertions.assertTrue(
          secondPassLatch.await(6, TimeUnit.SECONDS),
          "All parties should pass the second barrier, verifying automatic reset");
      Assertions.assertEquals(
          0, barrier.getNumberWaiting(), "Waiting count should be 0 after second pass");

    } finally {
      barrier.close();
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
