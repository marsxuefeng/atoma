package atoma.test.barrier;

import atoma.api.BrokenBarrierException;
import atoma.api.synchronizer.CyclicBarrier;
import atoma.client.AtomaClient;
import atoma.storage.mongo.MongoCoordinationStore;
import atoma.test.BaseTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test case for BARRIER-TC-022 and BARRIER-TC-023: Behavior of await() when reset() is called.
 */
public class BarrierTc022And023Test extends BaseTest {

  @DisplayName("BARRIER-TC-022 & 023: 重置期间的await()行为")
  @Test
  public void testAwaitBehaviorDuringReset() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);

    final String barrierId = "BARRIER-TC-022";
    final int parties = 2;
    final CyclicBarrier barrier = client.getCyclicBarrier(barrierId, parties);

    final CountDownLatch exceptionCaught = new CountDownLatch(1);
    final AtomicReference<Throwable> party1Exception = new AtomicReference<>();

    Thread party1 = null;
    try {
      // Party 1 starts waiting
      party1 =
          new Thread(
              () -> {
                try {
                  barrier.await(10, TimeUnit.SECONDS);
                } catch (Exception e) {
                  party1Exception.set(e);
                } finally {
                  exceptionCaught.countDown();
                }
              });
      party1.start();

      // Ensure party1 is waiting
      TimeUnit.MILLISECONDS.sleep(500);
      Assertions.assertEquals(1, barrier.getNumberWaiting(), "One party should be waiting");

      // Another client/thread resets the barrier
      barrier.reset();

      // The waiting party should receive a BrokenBarrierException
      Assertions.assertTrue(
          exceptionCaught.await(5, TimeUnit.SECONDS), "Waiting party should have thrown");
      Assertions.assertInstanceOf(
          BrokenBarrierException.class,
          party1Exception.get(),
          "Waiting party should receive BrokenBarrierException after reset");

      // The barrier should now be in a broken state
      Assertions.assertTrue(barrier.isBroken(), "Barrier should be broken after reset");

      // A new party (TC-022) attempting to await should also fail immediately
      Assertions.assertThrows(
          BrokenBarrierException.class,
          () -> barrier.await(),
          "New party awaiting on broken barrier should fail immediately");

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
