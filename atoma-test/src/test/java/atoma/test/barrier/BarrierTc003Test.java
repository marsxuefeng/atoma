package atoma.test.barrier;

import atoma.client.AtomaClient;
import atoma.storage.mongo.MongoCoordinationStore;
import atoma.test.BaseTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Test case for BARRIER-TC-003: Initialize barrier with a negative number of parties.
 */
public class BarrierTc003Test extends BaseTest {

  @DisplayName("BARRIER-TC-003: 使用负数的参与者数量初始化屏障")
  @Test
  public void testInitializeWithNegativeParties() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);

    final String barrierId = "BARRIER-TC-003";
    final int parties = -1;

    try {
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> client.getCyclicBarrier(barrierId, parties),
          "Initializing with negative parties should throw IllegalArgumentException");
    } finally {
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
