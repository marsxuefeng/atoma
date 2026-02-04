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

/**
 * Test case for BARRIER-TC-017: A node's await() times out, other nodes are normal.
 *
 * @see java.util.concurrent.BrokenBarrierException
 * @see java.util.concurrent.CyclicBarrier
 */
public class BarrierTc017Test extends BaseTest {

  @DisplayName("BARRIER-TC-017: 节点await()超时，其他节点正常")
  @Test
  public void testSingleNodeTimeout() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);
    Lease lease = client.grantLease();

    // CORRECTED LOGIC: Set parties to 3, but only start 2 parties.
    // This ensures the barrier will wait until a timeout occurs.
    final String barrierId = "BARRIER-TC-017";
    final int parties = 3;
    final CyclicBarrier barrier = lease.getCyclicBarrier(barrierId, parties);

    final CountDownLatch exceptionsCaught = new CountDownLatch(2);
    final AtomicReference<Throwable> party1Exception = new AtomicReference<>();
    final AtomicReference<Throwable> party2Exception = new AtomicReference<>();

    try {
      // Party 1: Waits with a long timeout, expects BrokenBarrierException
      Thread party1 =
          new Thread(
              () -> {
                try {
                  // This party will wait until the barrier is broken by party 2.
                  barrier.await(10, TimeUnit.SECONDS);
                } catch (Exception e) {
                  e.printStackTrace();
                  party1Exception.set(e);
                } finally {
                  exceptionsCaught.countDown();
                }
              });

      // Party 2: Waits with a short timeout, expects TimeoutException
      Thread party2 =
          new Thread(
              () -> {
                try {
                  // This party will time out first, as the 3rd party never arrives.
                  barrier.await(1, TimeUnit.SECONDS);
                } catch (Exception e) {
                  System.err.println("Part2 退出 " + e.getMessage());
                  party2Exception.set(e);
                } finally {
                  exceptionsCaught.countDown();
                }
              });

      party1.start();
      // Ensure party1 is waiting before party2 starts
      TimeUnit.MILLISECONDS.sleep(200);
      party2.start();

      // The 3rd party never arrives.

      Assertions.assertTrue(
          exceptionsCaught.await(12, TimeUnit.SECONDS), "Both parties should catch an exception");

      TimeUnit.SECONDS.sleep(12);

      // Party 2 timed out first, breaking the barrier for Party 1.
      Assertions.assertNotNull(party2Exception.get(), "Party 2 should have thrown an exception");
      Assertions.assertInstanceOf(
          TimeoutException.class, party2Exception.get(), "Party 2 should receive TimeoutException");

      Assertions.assertNotNull(party1Exception.get(), "Party 1 should have thrown an exception");
      Assertions.assertInstanceOf(
          BrokenBarrierException.class,
          party1Exception.get(),
          "Party 1 should receive BrokenBarrierException");

      Assertions.assertTrue(barrier.isBroken(), "Barrier should be in a broken state");

    } finally {
      barrier.close();
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
