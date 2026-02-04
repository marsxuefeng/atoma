package atoma.test.barrier;

import atoma.api.BrokenBarrierException;
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

/** Test case for BARRIER-TC-020: Verify barrier state after a timeout. */
public class BarrierTc020Test extends BaseTest {

  @DisplayName("BARRIER-TC-020: 超时后屏障状态验证")
  @Test
  public void testBarrierStateAfterTimeout() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);
    Lease lease = client.grantLease();
    final String barrierId = "BARRIER-TC-020";
    final int parties = 2;
    final CyclicBarrier barrier = lease.getCyclicBarrier(barrierId, parties);
    final CountDownLatch timeoutLatch = new CountDownLatch(1);

    Thread party1 = null;
    try {
      // Party 1 awaits with a short timeout to break the barrier
      party1 =
          new Thread(
              () -> {
                Assertions.assertThrows(
                    TimeoutException.class,
                    () -> barrier.await(1, TimeUnit.SECONDS),
                    "Party 1 should time out");
                timeoutLatch.countDown();
              });
      party1.start();

      // Wait for the timeout to occur
      Assertions.assertTrue(
          timeoutLatch.await(2, TimeUnit.SECONDS), "Timeout should have occurred");

      // Verify the barrier is now broken
      Assertions.assertTrue(barrier.isBroken(), "Barrier should be in a broken state");

      // Party 2 attempts to await on the now-broken barrier
      Assertions.assertThrows(
          BrokenBarrierException.class,
          () -> barrier.await(),
          "Awaiting on a broken barrier should immediately throw BrokenBarrierException");

    } finally {
      if (party1 != null) {
        party1.join();
      }
      barrier.close();
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
