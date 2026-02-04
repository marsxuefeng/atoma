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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Test case for BARRIER-TC-033: High concurrency nodes simultaneously await(). */
public class BarrierTc033Test extends BaseTest {

  @DisplayName("BARRIER-TC-033: 高并发节点同时await()")
  @Test
  public void testHighConcurrencyAwait() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);
    Lease lease = client.grantLease();
    final String barrierId = "BARRIER-TC-033";
    final int parties = 10; // Using a higher number of parties for concurrency test
    final CyclicBarrier barrier = lease.getCyclicBarrier(barrierId, parties);
    final CountDownLatch passLatch = new CountDownLatch(parties);
    ExecutorService threadPool = Executors.newFixedThreadPool(parties);

    try {
      for (int i = 0; i < parties; i++) {
        threadPool.submit(
            () -> {
              try {
                barrier.await(10, TimeUnit.SECONDS);
                passLatch.countDown();
              } catch (Exception e) {
                Assertions.fail("Await should not fail under high concurrency", e);
              }
            });
      }

      Assertions.assertTrue(
          passLatch.await(15, TimeUnit.SECONDS), "All concurrent parties should pass the barrier");
      Assertions.assertEquals(
          0,
          barrier.getNumberWaiting(),
          "Number of waiting parties should be zero after barrier is passed");

    } finally {
      threadPool.shutdownNow();
      barrier.close();
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
