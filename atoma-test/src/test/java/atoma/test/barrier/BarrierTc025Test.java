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

/** Test case for BARRIER-TC-025: A single node fails before await(). */
public class BarrierTc025Test extends BaseTest {

  @DisplayName("BARRIER-TC-025: 单个节点在await()前失败")
  @Test
  public void testNodeFailsBeforeAwait() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);
    Lease lease = client.grantLease();
    final String barrierId = "BARRIER-TC-025";
    final int parties = 3;
    final CyclicBarrier barrier = lease.getCyclicBarrier(barrierId, parties);

    final CountDownLatch exceptionLatch = new CountDownLatch(parties - 1);

    try {
      // Start N-1 parties that will wait
      for (int i = 0; i < parties - 1; i++) {
        new Thread(
                () -> {
                  // These parties will wait and eventually time out or get a BrokenBarrierException
                  Assertions.assertThrows(
                      Exception.class,
                      () -> barrier.await(2, TimeUnit.SECONDS),
                      "Waiting parties should throw an exception");
                  exceptionLatch.countDown();
                })
            .start();
      }

      // The last party "fails" by never calling await.

      Assertions.assertTrue(
          exceptionLatch.await(5, TimeUnit.SECONDS),
          "All waiting parties should have thrown an exception");
      Assertions.assertTrue(barrier.isBroken(), "Barrier should be broken due to timeout");

    } finally {
      barrier.close();
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
