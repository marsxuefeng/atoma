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

/** Test case for BARRIER-TC-016: await() calls with mixed client-side delays. */
public class BarrierTc016Test extends BaseTest {

  @DisplayName("BARRIER-TC-016: 混合客户端延迟下的await()调用")
  @Test
  public void testAwaitWithMixedDelays() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);
    Lease lease = client.grantLease();

    final String barrierId = "BARRIER-TC-016";
    final int parties = 3;
    final CyclicBarrier barrier = lease.getCyclicBarrier(barrierId, parties);
    final CountDownLatch passLatch = new CountDownLatch(parties);

    try {
      // Thread 1: No delay
      new Thread(
              () -> {
                try {
                  barrier.await(5, TimeUnit.SECONDS);
                  passLatch.countDown();
                } catch (Exception e) {
                  Assertions.fail("Await should not fail", e);
                }
              })
          .start();

      // Thread 2: 100ms delay
      new Thread(
              () -> {
                try {
                  TimeUnit.MILLISECONDS.sleep(100);
                  barrier.await(5, TimeUnit.SECONDS);
                  passLatch.countDown();
                } catch (Exception e) {
                  Assertions.fail("Await should not fail", e);
                }
              })
          .start();

      // Thread 3: 200ms delay
      new Thread(
              () -> {
                try {
                  TimeUnit.MILLISECONDS.sleep(200);
                  barrier.await(5, TimeUnit.SECONDS);
                  passLatch.countDown();
                } catch (Exception e) {
                  Assertions.fail("Await should not fail", e);
                }
              })
          .start();

      Assertions.assertTrue(
          passLatch.await(6, TimeUnit.SECONDS),
          "All parties should pass the barrier despite delays");
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
