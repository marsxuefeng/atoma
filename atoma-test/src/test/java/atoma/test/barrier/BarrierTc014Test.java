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
import java.util.concurrent.atomic.AtomicBoolean;

/** Test case for BARRIER-TC-014: A single node calls await() and waits for other nodes. */
public class BarrierTc014Test extends BaseTest {

  @DisplayName("BARRIER-TC-014: 单个节点调用await()并等待其他节点")
  @Test
  public void testSingleNodeWaitsForOthers() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);
    Lease lease = client.grantLease();

    final String barrierId = "BARRIER-TC-014";
    final int parties = 2;
    final CyclicBarrier barrier = lease.getCyclicBarrier(barrierId, parties);
    final CountDownLatch passLatch = new CountDownLatch(parties);
    final AtomicBoolean firstPartyFinished = new AtomicBoolean(false);

    try {
      // Start the first party
      Thread firstParty =
          new Thread(
              () -> {
                try {
                  barrier.await(5, TimeUnit.SECONDS);
                  firstPartyFinished.set(true);
                  passLatch.countDown();
                } catch (Exception e) {
                  Assertions.fail("First party should not fail", e);
                }
              });
      firstParty.start();

      // Give the first party time to enter the await state
      TimeUnit.MILLISECONDS.sleep(500);
      Assertions.assertEquals(1, barrier.getNumberWaiting(), "One party should be waiting");
      Assertions.assertFalse(
          firstPartyFinished.get(), "First party should be waiting, not finished");

      // Start the second party
      new Thread(
              () -> {
                try {
                  barrier.await(5, TimeUnit.SECONDS);
                  passLatch.countDown();
                } catch (Exception e) {
                  Assertions.fail("Second party should not fail", e);
                }
              })
          .start();

      // Both parties should now pass
      Assertions.assertTrue(
          passLatch.await(6, TimeUnit.SECONDS), "All parties should pass the barrier");
      Assertions.assertTrue(firstPartyFinished.get(), "First party should have finished");
      Assertions.assertEquals(
          0,
          barrier.getNumberWaiting(),
          "Number of waiting parties should be zero after barrier is passed");

    } finally {
      barrier.close();
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
