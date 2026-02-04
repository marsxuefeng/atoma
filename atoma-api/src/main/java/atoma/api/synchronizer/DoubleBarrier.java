package atoma.api.synchronizer;

import atoma.api.Resourceful;

/**
 * A synchronization aid that allows a set of threads to all wait for each other to reach two
 * distinct barrier points (an entry barrier and an exit barrier).
 *
 * <p>A {@code DoubleBarrier} is useful in situations where a group of threads needs to perform a
 * collective operation, then proceed, and then perform another collective operation before
 * completely dispersing.
 */
public abstract class DoubleBarrier extends Resourceful {

  /**
   * Returns the number of parties currently involved in this barrier.
   *
   * @return the number of parties currently involved in this barrier.
   */
  public abstract int getParticipants();

  /**
   * Waits until all parties have invoked {@code enter} on this barrier.
   *
   * <p>If the current thread is not the last to arrive then it is disabled for thread scheduling
   * purposes and lies dormant until all other parties have also invoked {@code enter}.
   *
   * @throws InterruptedException if the current thread was interrupted while waiting
   */
  public abstract void enter() throws InterruptedException;

  /**
   * Waits until all parties have invoked {@code leave} on this barrier.
   *
   * <p>If the current thread is not the last to leave then it is disabled for thread scheduling
   * purposes and lies dormant until all other parties have also invoked {@code leave}.
   *
   * @throws InterruptedException if the current thread was interrupted while waiting
   */
  public abstract void leave() throws InterruptedException;
}
