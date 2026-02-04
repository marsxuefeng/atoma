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

import atoma.api.Resourceful;

import java.util.concurrent.TimeUnit;

/**
 * A distributed synchronization aid that allows one or more threads to wait until a set of
 * operations being performed in other threads have completed.
 *
 * <p>A {@code CountDownLatch} is initialized with a given count. The {@link #countDown()} method
 * decrements the count. Threads invoking {@link #await()} are blocked until the count reaches zero.
 * Once the count reaches zero, all waiting threads are released, and any subsequent invocations of
 * {@link #await()} return immediately.
 *
 * <p>This distributed version ensures that the count and waiting mechanisms are coordinated across
 * multiple processes or machines interacting with the Atoma coordination service.
 *
 * @see java.util.concurrent.CountDownLatch
 */
public abstract class CountDownLatch extends Resourceful {
  /**
   * Decrements the count of the latch. If the count reaches zero, all waiting threads are released.
   */
  public abstract void countDown();

  /**
   * Causes the current thread to wait until the latch has counted down to zero, unless the thread
   * is {@linkplain Thread#interrupt interrupted}.
   *
   * @throws InterruptedException if the current thread is interrupted while waiting.
   */
  public abstract void await() throws InterruptedException;

  /**
   * Causes the current thread to wait until the latch has counted down to zero, unless the thread
   * is {@linkplain Thread#interrupt interrupted}, or the specified waiting time elapses.
   *
   * @param timeout timeout the maximum time to wait
   * @param unit unit the time unit of the {@code timeout} argument
   * @return {@code true} if the count reached zero and {@code false} if the waiting time elapsed
   *     before the count reached zero
   * @throws InterruptedException if the current thread is interrupted while waiting.
   */
  public abstract boolean await(long timeout, TimeUnit unit) throws InterruptedException;

  /**
   * Returns the current count.
   *
   * <p>This method is typically used for debugging and testing purposes.
   *
   * @return The current count.
   */
  public abstract int getCount();

  /**
   * Deletes the latch resource from the backend coordination service. This is useful for explicit
   * resource cleanup when the latch is no longer needed and its underlying storage should be
   * removed.
   */
  public abstract void destroy();
}