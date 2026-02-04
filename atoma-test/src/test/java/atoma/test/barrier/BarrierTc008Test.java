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

/** Test case for BARRIER-TC-008: Node successfully registers to the barrier. */
public class BarrierTc008Test extends BaseTest {

  @DisplayName("BARRIER-TC-008: 节点成功注册到屏障")
  @Test
  public void testNodeSuccessfulRegistration() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);
    Lease lease = client.grantLease();

    final String barrierId = "BARRIER-TC-008";
    final int parties = 2;
    CyclicBarrier barrier = lease.getCyclicBarrier(barrierId, parties);

    CountDownLatch awaitStarted = new CountDownLatch(1);
    AtomicInteger waitingCount = new AtomicInteger(-1);

    Thread t =
        new Thread(
            () -> {
              try {
                awaitStarted.countDown();
                barrier.await(10, TimeUnit.SECONDS);
              } catch (Exception e) {
                // We expect a timeout here as the second party never arrives.
                e.printStackTrace();
              }
            });

    try {
      t.start();
      // Wait until the thread has called await()
      Assertions.assertTrue(
          awaitStarted.await(2, TimeUnit.SECONDS), "Await should have been called");

      // Give some time for the command to reach the server
      TimeUnit.MILLISECONDS.sleep(200);

      waitingCount.set(barrier.getNumberWaiting());
      Assertions.assertEquals(1, waitingCount.get(), "One node should be registered and waiting");

    } finally {
      t.interrupt(); // Clean up the waiting thread
      t.join();
      barrier.close();
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
