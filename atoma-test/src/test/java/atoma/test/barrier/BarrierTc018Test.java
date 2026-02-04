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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test case for BARRIER-TC-018: All nodes' await() calls time out.
 */
public class BarrierTc018Test extends BaseTest {

  @DisplayName("BARRIER-TC-018: 所有节点await()都超时")
  @Test
  public void testAllNodesTimeout() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);
    Lease lease = client.grantLease();

    // CORRECTED LOGIC: Set parties to 3, but only start 2 parties.
    final String barrierId = "BARRIER-TC-018";
    final int parties = 3;
    final int participatingParties = 2;
    final CyclicBarrier barrier = lease.getCyclicBarrier(barrierId, parties);

    final CountDownLatch threadsFinished = new CountDownLatch(participatingParties);
    final AtomicInteger exceptionCount = new AtomicInteger(0);

    try {
      for (int i = 0; i < participatingParties; i++) {
        new Thread(
                () -> {
                  try {
                    // These parties will wait and eventually time out as the 3rd party never arrives
                    barrier.await(1, TimeUnit.SECONDS);
                  } catch (Exception e) {
                    // Exceptions (Timeout or BrokenBarrier) are expected
                    exceptionCount.incrementAndGet();
                  } finally {
                    threadsFinished.countDown();
                  }
                })
            .start();
      }

      Assertions.assertTrue(
          threadsFinished.await(5, TimeUnit.SECONDS), "All threads should finish and not hang");
      Assertions.assertEquals(
          participatingParties,
          exceptionCount.get(),
          "All participating threads should have caught an exception");
      Assertions.assertTrue(barrier.isBroken(), "Barrier should be broken after timeouts");

    } finally {
      barrier.close();
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
