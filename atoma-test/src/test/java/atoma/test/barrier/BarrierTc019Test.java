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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/** Test case for BARRIER-TC-019: Mixed test with nodes having different timeout settings. */
public class BarrierTc019Test extends BaseTest {

  @DisplayName("BARRIER-TC-019: 设置不同超时时间的节点混合测试")
  @Test
  public void testMixedTimeouts() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);
      Lease lease = client.grantLease();
    final String barrierId = "BARRIER-TC-019";
    final int parties = 3;
    final CyclicBarrier barrier = lease.getCyclicBarrier(barrierId, parties);

    final CountDownLatch exceptionsCaught = new CountDownLatch(parties);
    final AtomicReference<Throwable> shortTimeoutException = new AtomicReference<>();
    final AtomicReference<Throwable> mediumTimeoutException = new AtomicReference<>();
    final AtomicReference<Throwable> longTimeoutException = new AtomicReference<>();

    try {
      // Party 1: Short timeout (1s) -> should throw TimeoutException
      Thread party1 =
          new Thread(
              () -> {
                try {
                  barrier.await(1, TimeUnit.SECONDS);
                } catch (Exception e) {
                  shortTimeoutException.set(e);
                } finally {
                  exceptionsCaught.countDown();
                }
              });

      // Party 2: Medium timeout (5s) -> should throw BrokenBarrierException
      Thread party2 =
          new Thread(
              () -> {
                try {
                  barrier.await(5, TimeUnit.SECONDS);
                } catch (Exception e) {
                  mediumTimeoutException.set(e);
                  e.printStackTrace();
                } finally {
                  exceptionsCaught.countDown();
                }
              });

      // Party 3: Long timeout (10s) -> should throw BrokenBarrierException
      Thread party3 =
          new Thread(
              () -> {
                try {
                  barrier.await(10, TimeUnit.SECONDS);
                } catch (Exception e) {
                  longTimeoutException.set(e);
                    e.printStackTrace();
                } finally {
                  exceptionsCaught.countDown();
                }
              });

      // Start all parties, ensuring the ones with longer timeouts start first
      // to make it more likely they are already waiting when the short one times out.

      party1.start();
      TimeUnit.SECONDS.sleep(2); // Small delay
      party3.start();
      party2.start();

      Assertions.assertTrue(
          exceptionsCaught.await(12, TimeUnit.SECONDS), "All parties should catch an exception");

      Assertions.assertInstanceOf(
          TimeoutException.class,
          shortTimeoutException.get(),
          "Shortest timeout party should receive TimeoutException");
      Assertions.assertInstanceOf(
          BrokenBarrierException.class,
          mediumTimeoutException.get(),
          "Medium timeout party should receive BrokenBarrierException");
      Assertions.assertInstanceOf(
          BrokenBarrierException.class,
          longTimeoutException.get(),
          "Long timeout party should receive BrokenBarrierException");

      Assertions.assertTrue(barrier.isBroken(), "Barrier should be in a broken state");

    } finally {
      barrier.close();
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
