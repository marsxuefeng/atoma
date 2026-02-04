/*
 * Copyright 2025 XueFeng Ma
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package atoma.api.synchronizer;

import atoma.api.BrokenBarrierException;
import atoma.api.Leasable;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A synchronization aid that allows a set of threads to all wait for each other to reach a common
 * barrier point. CyclicBarriers are useful in programs involving a fixed size group of threads that
 * must occasionally wait for each other. The barrier is called cyclic because it can be re-used
 * after the waiting threads are released.
 */
public abstract class CyclicBarrier extends Leasable {

  /**
   * Waits until all parties have invoked await on this barrier, or the specified waiting time
   * elapses.
   *
   * <p>If the current thread is not the last to arrive then it is disabled for thread scheduling
   * purposes and lies dormant until one of the following things happens:
   *
   * <ul>
   *   <li>The last thread arrives; or
   *   <li>Some other thread interrupts the current thread; or
   *   <li>Some other thread interrupts one of the other waiting threads; or
   *   <li>Some other thread times out while waiting for the barrier; or
   *   <li>Some other thread invokes {@link #reset()} on this barrier.
   * </ul>
   *
   * @param timeout the time to wait for the barrier
   * @param unit the time unit of the timeout parameter
   * @throws InterruptedException if the current thread was interrupted while waiting
   * @throws BrokenBarrierException if another thread was interrupted or timed out while the current
   *     thread was waiting, or the barrier was reset, or the barrier was broken when await was
   *     called, or the barrier action (if applicable) failed
   * @throws TimeoutException if the specified timeout elapses
   */
  public abstract void await(long timeout, TimeUnit unit)
      throws InterruptedException, BrokenBarrierException, TimeoutException;

  /**
   * Waits until all parties have invoked await on this barrier.
   *
   * <p>If the current thread is not the last to arrive then it is disabled for thread scheduling
   * purposes and lies dormant until one of the following things happens:
   *
   * <ul>
   *   <li>The last thread arrives; or
   *   <li>Some other thread interrupts the current thread; or
   *   <li>Some other thread interrupts one of the other waiting threads; or
   *   <li>Some other thread invokes {@link #reset()} on this barrier.
   * </ul>
   *
   * @throws InterruptedException if the current thread was interrupted while waiting
   * @throws BrokenBarrierException if another thread was interrupted or timed out while the current
   *     thread was waiting, or the barrier was reset, or the barrier was broken when await was
   *     called, or the barrier action (if applicable) failed
   */
  public abstract void await() throws InterruptedException, BrokenBarrierException;

  /**
   * Resets the barrier to its initial state. If any parties are currently waiting at the barrier,
   * they will be released with a {@link BrokenBarrierException}. Note that resets after a breakage
   * has occurred for other reasons can be complicated to carry out; threads need to re-synchronize
   * their actions, and it may be often preferrable to create a new barrier instead.
   */
  public abstract void reset();

  /**
   * Queries if this barrier is in a broken state.
   *
   * @return {@code true} if one or more parties broke out of this barrier due to interruption or
   *     timeout since construction or the last reset, or a barrier action failed, {@code false}
   *     otherwise.
   */
  public abstract boolean isBroken();

  /**
   * Returns the number of parties required to trip this barrier.
   *
   * @return the number of parties required to trip this barrier
   */
  public abstract int getParties();

  /**
   * Returns the number of parties currently waiting at the barrier. This method is primarily useful
   * for debugging and assertions.
   *
   * @return the number of parties currently waiting at the barrier
   */
  public abstract int getNumberWaiting();
}
