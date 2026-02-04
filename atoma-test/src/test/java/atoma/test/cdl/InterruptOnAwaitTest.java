package atoma.test.cdl;

import atoma.api.synchronizer.CountDownLatch;
import atoma.core.AtomaClient;
import atoma.storage.mongo.MongoCoordinationStore;
import atoma.test.BaseTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ScheduledExecutorService;

public class InterruptOnAwaitTest extends BaseTest {

  @DisplayName("DCL-TC-011: 在await()时中断线程")
  @Test
  public void testInterruptOnAwait() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);
    CountDownLatch latch = client.getCountDownLatch("TestCountDown-011", 1);

    Thread t =
        new Thread(
            () -> {
              try {
                latch.await();
                Assertions.fail("Should have thrown InterruptedException");
              } catch (InterruptedException e) {

                // 当以下情况发生时，JVM会自动清除线程的中断状态：
                // 1. 抛出InterruptedException时：
                //  - 当线程在阻塞方法（如sleep()、wait()、join()、await()等）中被中断
                //  - JVM会抛出InterruptedException并自动清除中断状态
                // 2. 调用Thread.interrupted()时：
                //  - 这个方法会检查并清除当前线程的中断状态
                //  - 返回线程之前的中断状态，然后将其设置为false
                // 目的是让线程知道它已经响应了中断请求
                Assertions.assertFalse(Thread.currentThread().isInterrupted());
              }
            });

    try {
      t.start();
      Thread.sleep(500); // Give the thread time to start awaiting
      t.interrupt();
      t.join(1000); // Wait for the thread to finish
      Assertions.assertFalse(t.isAlive());
    } finally {
      latch.close();
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
