package atoma.test.barrier;

import atoma.client.AtomaClient;
import atoma.storage.mongo.MongoCoordinationStore;
import atoma.test.BaseTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ScheduledExecutorService;

/** Test case for BARRIER-TC-004: Initialize barrier with zero parties. */
public class BarrierTc004Test extends BaseTest {

  @DisplayName("BARRIER-TC-004: 使用零作为参与者数量初始化屏障")
  @Test
  public void testInitializeWithZeroParties() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);

    final String barrierId = "BARRIER-TC-004";
    final int parties = 0;

    try {
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> client.getCyclicBarrier(barrierId, parties),
          "Initializing with zero parties should throw IllegalArgumentException");
    } finally {
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
