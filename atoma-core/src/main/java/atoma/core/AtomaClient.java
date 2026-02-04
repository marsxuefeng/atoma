package atoma.core;

import atoma.api.AtomaStateException;
import atoma.api.Lease;
import atoma.api.Resourceful;
import atoma.api.coordination.CoordinationStore;
import atoma.api.coordination.command.CleanDeadResourceCommand;
import atoma.api.synchronizer.CountDownLatch;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.collect.Tables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.errorprone.annotations.MustBeClosed;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class AtomaClient implements AutoCloseable {
  private final boolean ownExecutor;
  private final CoordinationStore coordinationStore;

  private final Table<Class<? extends Resourceful>, String, Resourceful> atomaResources =
      Tables.synchronizedTable(HashBasedTable.create());

  private final ScheduledExecutorService scheduleExecutor;

  public AtomaClient(CoordinationStore coordinationStore) {
    this.coordinationStore = coordinationStore;
    this.scheduleExecutor =
        Executors.newScheduledThreadPool(
            8, new ThreadFactoryBuilder().setNameFormat("atoma-ttl-worker-%d").build());
    this.ownExecutor = true;
    startTTLTask();
  }

  public AtomaClient(ScheduledExecutorService ttlExecutor, CoordinationStore coordinationStore) {
    this.coordinationStore = coordinationStore;
    this.scheduleExecutor = ttlExecutor;
    this.ownExecutor = false;
    startTTLTask();
  }

  private void startTTLTask() {
    CleanDeadResourceCommand.Clean cleanCommand = new CleanDeadResourceCommand.Clean(null);
    coordinationStore.execute("", cleanCommand);
    scheduleExecutor.scheduleAtFixedRate(
        () -> {
          try {
            coordinationStore.execute("", cleanCommand);
          } catch (Throwable ignore) {
          }
        },
        0,
        2,
        TimeUnit.SECONDS);
  }

  public Lease grantLease() {
    return grantLease(Duration.ofSeconds(32));
  }

  public Lease grantLease(Duration ttl) {
    Lease lease =
        new DefaultLease(
            scheduleExecutor,
            coordinationStore,
            ttl,
            (t) -> {
              DefaultLease removedLease =
                  (DefaultLease) atomaResources.remove(Lease.class, t.getResourceId());

              if (removedLease != null) {
                CleanDeadResourceCommand.Clean cleanCommand =
                    new CleanDeadResourceCommand.Clean(removedLease.getResourceId());
                coordinationStore.execute("", cleanCommand);
              }
            });
    this.atomaResources.put(Lease.class, lease.getResourceId(), lease);
    return lease;
  }

  @MustBeClosed
  public CountDownLatch getCountDownLatch(String resourceId, int count) {
    CountDownLatch countDownLatch =
        (CountDownLatch) atomaResources.get(CountDownLatch.class, resourceId);
    if (countDownLatch == null) {
      countDownLatch = new DefaultCountDownLatch(resourceId, count, this.coordinationStore);
      atomaResources.put(CountDownLatch.class, resourceId, countDownLatch);
    }
    return countDownLatch;
  }

  @Override
  public synchronized void close() throws Exception {
    if (ownExecutor) {
      scheduleExecutor.shutdown();
    }

    // Avoid ConcurrentModifyException
    HashBasedTable<Class<? extends Resourceful>, String, Resourceful> atomaResoucesCopier =
        HashBasedTable.create(atomaResources);
    atomaResoucesCopier
        .values()
        .forEach(
            resourceful -> {
              try {
                // Which will be modify atomaResources
                resourceful.close();
              } catch (Exception e) {
                throw new AtomaStateException(e);
              }
            });
  }
}
