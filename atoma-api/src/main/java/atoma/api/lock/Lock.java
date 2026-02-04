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

package atoma.api.lock;

import atoma.api.Leasable;
import atoma.api.IllegalOwnershipException;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A {@code Lock} is a tool for controlling access to a shared resource by multiple threads. Commonly,
 * a lock provides exclusive access to a shared resource: only one thread at a time can acquire the
 * lock and all access to the shared resource is required to be preceded by acquiring the lock.
 *
 * <p>A lock is a {@link Leasable} resource, meaning it is acquired for a specific duration (lease
 * time). If the holder of the lock does not renew the lease before it expires, the lock is
 * automatically released, allowing other threads to acquire it. This mechanism prevents deadlocks
 * caused by a thread crashing or becoming unresponsive while holding a lock.
 *
 * <p>It is recommended to wrap lock acquisition and release in a {@code try-finally} block to ensure
 * the lock is released even in case of an exception.
 *
 * <pre>{@code
 * Lock lock = ...;
 * lock.lock();
 * try {
 *   // access the protected resource
 * } finally {
 *   lock.unlock();
 * }
 * }</pre>
 *
 * <p>Implementations of this class are expected to be thread-safe.
 */
public abstract class Lock extends Leasable {

  /**
   * Acquires the lock.
   *
   * <p>If the lock is not available then the current thread becomes disabled for thread scheduling
   * purposes and lies dormant until the lock has been acquired.
   *
   * @throws InterruptedException if the current thread is interrupted while acquiring the lock
   */
  public abstract void lock() throws InterruptedException;

  /**
   * Acquires the lock if it is free within the given waiting time and the current thread has not
   * been {@linkplain Thread#interrupt interrupted}.
   *
   * <p>If the lock is available, this method returns immediately. If the lock is not available, the
   * current thread becomes disabled for thread scheduling purposes and lies dormant until one of
   * three things happens:
   *
   * <ul>
   *   <li>The lock is acquired by the current thread; or
   *   <li>Some other thread {@linkplain Thread#interrupt interrupts} the current thread; or
   *   <li>The specified waiting time elapses.
   * </ul>
   *
   * @param time the maximum time to wait for the lock
   * @param unit the time unit of the {@code time} argument
   * @throws InterruptedException if the current thread is interrupted while acquiring the lock
   * @throws TimeoutException if the waiting time elapses before the lock could be acquired
   */
  public abstract void lock(long time, TimeUnit unit) throws InterruptedException, TimeoutException;

  /**
   * Acquires the lock unless the current thread is {@linkplain Thread#interrupt interrupted}.
   *
   * <p>Acquires the lock if it is available and returns immediately.
   *
   * <p>If the lock is not available then the current thread becomes disabled for thread scheduling
   * purposes and lies dormant until one of two things happens:
   *
   * <ul>
   *   <li>The lock is acquired by the current thread; or
   *   <li>Some other thread {@linkplain Thread#interrupt interrupts} the current thread.
   * </ul>
   *
   * @throws InterruptedException if the current thread is interrupted while acquiring the lock
   */
  public abstract void lockInterruptibly() throws InterruptedException;

  /**
   * Releases the lock.
   *
   * <p><strong>Implementation Note:</strong> A {@code Lock} implementation will usually impose
   * restrictions on which thread can release a lock (typically only the holder of the lock can
   * release it) and may throw an {@link IllegalOwnershipException} (an unchecked exception) if the
   * restriction is violated.
   */
  public abstract void unlock();
}