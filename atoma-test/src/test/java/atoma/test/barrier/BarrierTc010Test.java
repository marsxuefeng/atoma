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

/** Test case for BARRIER-TC-010: More nodes than parties attempt to register. */
public class BarrierTc010Test extends BaseTest {

  @DisplayName("BARRIER-TC-010: 超过参与者数量的节点尝试注册")
  @Test
  public void testMoreNodesThanParties() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);
    Lease lease = client.grantLease();

    final String barrierId = "BARRIER-TC-010";
    final int parties = 2;
    final CyclicBarrier barrier = lease.getCyclicBarrier(barrierId, parties);
    final CountDownLatch passLatch = new CountDownLatch(parties);
    final CountDownLatch thirdThreadStarted = new CountDownLatch(1);

    Thread thirdThread = null;
    try {
      // Start the first two threads that should pass the barrier
      for (int i = 0; i < parties; i++) {
        new Thread(
                () -> {
                  try {
                    barrier.await(5, TimeUnit.SECONDS);
                    passLatch.countDown();
                  } catch (Exception e) {
                    Assertions.fail("First " + parties + " parties should not fail", e);
                  }
                })
            .start();
      }

      // Wait for the first barrier to trip
      Assertions.assertTrue(passLatch.await(6, TimeUnit.SECONDS), "First barrier should be passed");
      TimeUnit.MILLISECONDS.sleep(200); // Allow time for barrier to reset
      Assertions.assertEquals(
          0, barrier.getNumberWaiting(), "Waiting count should be 0 after barrier pass");

      // Start the third thread, which will wait on the next generation
      thirdThread =
          new Thread(
              () -> {
                try {
                  thirdThreadStarted.countDown();
                  // This await will time out as no other party arrives
                  barrier.await(3, TimeUnit.SECONDS);
                } catch (Exception e) {
                  // Expected timeout
                }
              });
      thirdThread.start();

      // Wait for the third thread to call await
      Assertions.assertTrue(
          thirdThreadStarted.await(2, TimeUnit.SECONDS), "Third thread should start");
      TimeUnit.MILLISECONDS.sleep(200); // Allow time for await command to be processed

      // Verify that the third thread is now waiting
      Assertions.assertEquals(
          1, barrier.getNumberWaiting(), "One party should be waiting for the next generation");

    } finally {
      if (thirdThread != null) {
        thirdThread.interrupt();
        thirdThread.join();
      }
      barrier.close();
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
