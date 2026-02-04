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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Test case for BARRIER-TC-034: Mixed read/write operations (await, reset). */
public class BarrierTc034Test extends BaseTest {

  @DisplayName("BARRIER-TC-034: 混合await和reset操作")
  @Test
  public void testMixedAwaitAndReset() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);
    Lease lease = client.grantLease();
    final String barrierId = "BARRIER-TC-034";
    final int parties = 5;
    final CyclicBarrier barrier = lease.getCyclicBarrier(barrierId, parties);
    final CountDownLatch threadsFinished = new CountDownLatch(parties);
    final AtomicBoolean chaosDone = new AtomicBoolean(false);
    ExecutorService threadPool = Executors.newFixedThreadPool(parties + 1);

    try {
      // Start N parties that will await
      for (int i = 0; i < parties; i++) {
        threadPool.submit(
            () -> {
              try {
                // We expect this to be interrupted by a reset
                barrier.await(10, TimeUnit.SECONDS);
              } catch (BrokenBarrierException e) {
                // This is the expected outcome
              } catch (Exception e) {
                // Any other exception is a failure
                Assertions.fail("Should only throw BrokenBarrierException", e);
              } finally {
                threadsFinished.countDown();
              }
            });
      }

      // Start a "chaos" thread that repeatedly calls reset
      threadPool.submit(
          () -> {
            while (!chaosDone.get()) {
              barrier.reset();
              try {
                TimeUnit.MILLISECONDS.sleep(50);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            }
          });

      // Wait for all awaiting threads to finish (they should all be broken)
      boolean finished = threadsFinished.await(15, TimeUnit.SECONDS);
      chaosDone.set(true); // Stop the chaos thread

      Assertions.assertTrue(finished, "All awaiting threads should finish and not hang");

    } finally {
      threadPool.shutdownNow();
      barrier.close();
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
