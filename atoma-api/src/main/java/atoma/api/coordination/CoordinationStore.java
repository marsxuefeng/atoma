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

package atoma.api.coordination;

import atoma.api.Resourceful;
import atoma.api.coordination.command.Command;

import java.util.Optional;

/**
 * {@code CoordinationStore} is the central interface for interacting with the Atoma distributed
 * coordination service. It provides mechanisms to manage distributed resources, execute commands
 * against them, and subscribe to their change events.
 *
 * <p>Implementations of this interface are responsible for handling the persistent storage and
 * synchronization logic required for distributed concurrency primitives.
 *
 * <p>This interface extends {@link AutoCloseable}, implying that implementations may hold resources
 * that need to be explicitly released when the store is no longer needed.
 */
public interface CoordinationStore extends AutoCloseable {

  /**
   * Retrieves the current snapshot of a specified resource node.
   *
   * @param resourceId The unique identifier of the resource.
   * @return An {@link Optional} containing the {@link Resource} snapshot if it exists, or an empty
   *     {@link Optional} if the resource is not found.
   */
  Optional<Resource> get(String resourceId);

  /**
   * Subscribes to change events for a specific resource node.
   *
   * <p>When the specified resource is created, updated, or deleted, the provided {@link
   * ResourceListener} will be invoked.
   *
   * @param resourceType The type of resource to subscribe to (e.g., "mutexLock" for distributed
   *     locks, "readWriteLock" for read-write locks, "semaphore" for semaphores, "countDownLatch"
   *     for latches, etc.).
   * @param resourceId The unique key of the resource to subscribe to.
   * @param listener The {@link ResourceListener} to be invoked upon resource changes.
   * @return A {@link Subscription} object that can be used to cancel this subscription in the
   *     future, preventing further listener invocations.
   */
  Subscription subscribe(
      Class<? extends Resourceful> resourceType, String resourceId, ResourceListener listener);

  /**
   * Executes a specific {@link Command} against a particular distributed resource.
   *
   * <p>This method provides a generic way to interact with and modify the state of distributed
   * coordination primitives. The execution logic is typically handled by specific {@link
   * atoma.api.coordination.command.CommandHandler} implementations.
   *
   * @param resourceId The unique key of the target resource on which the command will be executed.
   * @param command The {@link Command} to execute, encapsulating the operation details.
   * @param <R> The type of the result expected from this command's execution.
   * @return A command-specific result object, which may contain the outcome of the operation or
   *     updated state information.
   */
  <R> R execute(String resourceId, Command<R> command);
}