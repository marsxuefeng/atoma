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
import java.util.concurrent.TimeoutException;

/** Test case for BARRIER-TC-028 & TC-029: Barrier recovery and rejoin after failure. */
public class BarrierTc028And029Test extends BaseTest {

  @DisplayName("BARRIER-TC-028 & 029: 节点失败后的恢复与重新加入")
  @Test
  public void testRecoveryAndRejoinAfterFailure() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);
    Lease lease = client.grantLease();
    final String barrierId = "BARRIER-TC-028";
    final int parties = 2;
    final CyclicBarrier barrier = lease.getCyclicBarrier(barrierId, parties);

    try {
      // 1. Simulate failure by having one party time out
      Assertions.assertThrows(
          TimeoutException.class,
          () -> barrier.await(1, TimeUnit.SECONDS),
          "A single party should time out");

      // 2. Verify the barrier is broken
      Assertions.assertTrue(barrier.isBroken(), "Barrier should be broken after timeout");

      // 3. Call reset() on the barrier
      barrier.reset();

      // 4. Verify the barrier is no longer broken
      // Give a moment for the reset command to propagate
      TimeUnit.MILLISECONDS.sleep(100);
      Assertions.assertFalse(barrier.isBroken(), "Barrier should not be broken after reset");

      // 5. Have all parties successfully pass the barrier
      final CountDownLatch passLatch = new CountDownLatch(parties);
      for (int i = 0; i < parties; i++) {
        new Thread(
                () -> {
                  try {
                    barrier.await(5, TimeUnit.SECONDS);
                    passLatch.countDown();
                  } catch (Exception e) {
                    Assertions.fail("Await should succeed after reset", e);
                  }
                })
            .start();
      }

      Assertions.assertTrue(
          passLatch.await(6, TimeUnit.SECONDS), "All parties should pass after reset");
      Assertions.assertEquals(
          0, barrier.getNumberWaiting(), "Waiting count should be 0 after successful pass");

    } finally {
      barrier.close();
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
