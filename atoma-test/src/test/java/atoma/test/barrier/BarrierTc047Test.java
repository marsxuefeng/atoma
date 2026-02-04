package atoma.test.barrier;

import atoma.api.Lease;
import atoma.api.synchronizer.CyclicBarrier;
import atoma.core.AtomaClient;
import atoma.storage.mongo.MongoCoordinationStore;
import atoma.test.BaseTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Test case for BARRIER-TC-047: Using a barrier ID with special characters. */
public class BarrierTc047Test extends BaseTest {

  @DisplayName("BARRIER-TC-047: 使用特殊字符的屏障ID")
  @ParameterizedTest
  @ValueSource(
      strings = {
        "barrier-with-hyphens",
        "barrier_with_underscores",
        "barrier.with.dots",
        "barrier/with/slashes",
        "barrier-ผสม-พิเศษ-😊"
      })
  public void testSpecialCharacterIds(String barrierId) throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);
    Lease lease = client.grantLease();
    final int parties = 2;
    final CyclicBarrier barrier = lease.getCyclicBarrier(barrierId, parties);
    final CountDownLatch passLatch = new CountDownLatch(parties);

    try {
      for (int i = 0; i < parties; i++) {
        new Thread(
                () -> {
                  try {
                    barrier.await(5, TimeUnit.SECONDS);
                    passLatch.countDown();
                  } catch (Exception e) {
                    Assertions.fail("Await should not fail for ID: " + barrierId, e);
                  }
                })
            .start();
      }

      Assertions.assertTrue(
          passLatch.await(6, TimeUnit.SECONDS), "All parties should pass for ID: " + barrierId);

    } finally {
      barrier.close();
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
