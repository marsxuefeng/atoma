package atoma.test.barrier;

import atoma.api.synchronizer.CyclicBarrier;
import atoma.client.AtomaClient;
import atoma.storage.mongo.MongoCoordinationStore;
import atoma.test.BaseTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Test case for BARRIER-TC-001: Single node initializes CyclicBarrier, sets a legal number of
 * parties.
 */
public class BarrierTc001Test extends BaseTest {

  @DisplayName("BARRIER-TC-001: 单节点初始化CyclicBarrier，设置合法参与者数量")
  @Test
  public void testInitializeBarrierWithLegalParties() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);

    final String barrierId = "BARRIER-TC-001";
    final int parties = 2;

    CyclicBarrier cyclicBarrier = null;
    try {
      cyclicBarrier = client.getCyclicBarrier(barrierId, parties);
      Assertions.assertNotNull(cyclicBarrier, "CyclicBarrier should not be null");
      Assertions.assertEquals(
          parties,
          cyclicBarrier.getParties(),
          "The number of parties should match the initialization value");
    } finally {
      if (cyclicBarrier != null) {
        cyclicBarrier.close();
      }
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
