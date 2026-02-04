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

public class SingleClientBarrierTest extends BaseTest {

  @DisplayName("BARRIER-TC-001: 单节点初始化CyclicBarrier，设置合法参与者数量")
  @Test
  public void testSingleClientCountDownLatchCount() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);
    Lease lease = client.grantLease();
    CyclicBarrier cyclicBarrier = lease.getCyclicBarrier("BARRIER-TC-001", 2);
    int clientCount = 2;

    CountDownLatch countDownLatch = new CountDownLatch(2);

    try {

      for (int i = 0; i < clientCount; i++) {
        new Thread(
                new Runnable() {
                  @Override
                  public void run() {
                    try {
                      System.err.printf(
                          "Thread %s-%d enter waiting....  %n",
                          Thread.currentThread().getName(), Thread.currentThread().getId());

                      TimeUnit.MILLISECONDS.sleep(200);

                      cyclicBarrier.await();
                      System.err.printf(
                          "Thread %s-%d enter waiting done....  %n",
                          Thread.currentThread().getName(), Thread.currentThread().getId());

                      countDownLatch.countDown();
                    } catch (InterruptedException e) {
                      throw new RuntimeException(e);
                    }
                  }
                })
            .start();
      }
      Assertions.assertTrue(countDownLatch.await(1, TimeUnit.SECONDS));

    } finally {
      cyclicBarrier.close();
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
