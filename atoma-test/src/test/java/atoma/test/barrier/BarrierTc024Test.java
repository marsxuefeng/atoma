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

/** Test case for BARRIER-TC-024: Verify barrier state consistency after multiple cycles. */
public class BarrierTc024Test extends BaseTest {

  @DisplayName("BARRIER-TC-024: 多次循环后屏障状态一致性验证")
  @Test
  public void testStateConsistencyAfterMultipleCycles() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);
    Lease lease = client.grantLease();
    final String barrierId = "BARRIER-TC-024";
    final int parties = 3;
    final int cycles = 5;
    final CyclicBarrier barrier = lease.getCyclicBarrier(barrierId, parties);

    try {
      for (int i = 1; i <= cycles; i++) {
        System.out.printf("--- Cycle %d ---%n", i);
        final CountDownLatch passLatch = new CountDownLatch(parties);

        for (int j = 0; j < parties; j++) {
          new Thread(
                  () -> {
                    try {
                      barrier.await(5, TimeUnit.SECONDS);
                      passLatch.countDown();
                    } catch (Exception e) {
                      Assertions.fail("Await should not fail during cycle ", e);
                    }
                  })
              .start();
        }

        Assertions.assertTrue(
            passLatch.await(6, TimeUnit.SECONDS), "All parties should pass cycle " + i);
        // Allow a moment for the state to propagate and reset.
        TimeUnit.MILLISECONDS.sleep(100);
        Assertions.assertEquals(
            0, barrier.getNumberWaiting(), "Waiting count should be 0 after cycle " + i);
        Assertions.assertFalse(barrier.isBroken(), "Barrier should not be broken after cycle " + i);
      }
    } finally {
      barrier.close();
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
